#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
import threading
from typing import Dict

from app_config.config_loader import Config
from joiner.worker import JoinerWorker
from middleware.middleware_client import (
    MessageMiddlewareExchange,
    MessageMiddlewareQueue,
)
from protocol import Opcodes


def ensure_joiner_bindings(cfg: Config, host: str, shard: int) -> None:
    """
    Declara la cola estable de cada tabla para este shard y asegura el binding
    exchange -> routing_key -> queue.
    """
    # Utilizamos MessageMiddlewareQueue para asegurarnos que la cola exista.
    # Luego creamos un publisher del exchange para forzar/asegurar el binding.
    tables = ["menu_items", "stores", "transactions", "transaction_items", "users"]
    for table in tables:
        rk = cfg.joiner_router_rk(table, shard)  # p.ej. join.stores.shard.15
        qn = cfg.joiner_queue(table, shard)  # p.ej. join.stores.shard.15
        ex = cfg.joiner_router_exchange(table)  # p.ej. jx.stores

        # 1) Declarar cola estable
        q = MessageMiddlewareQueue(host=host, queue_name=qn)
        # Cerramos el canal; la cola queda declarada en Rabbit
        try:
            q.close()
        except Exception:
            pass

        # 2) Asegurar el binding exchange<->rk->queue
        # Muchas implementaciones de Exchange crean el binding al pasar queue_name.
        # Si tu wrapper no bindea por sí solo, crea un consumidor efímero para forzar el bind.
        _tmp_consumer = MessageMiddlewareExchange(
            host=host,
            exchange_name=ex,
            route_keys=[rk],
            consumer=True,
            queue_name=qn,
        )
        # No arrancamos el consumo; con abrir/cerrar alcanza para declarar/bindear
        try:
            _tmp_consumer.close()
        except Exception:
            pass


def build_inputs_for_shard(
    cfg: Config, host: str, shard: int
) -> Dict[int, MessageMiddlewareExchange]:
    inputs: Dict[int, MessageMiddlewareExchange] = {}

    def make(table: str) -> MessageMiddlewareExchange:
        rk = cfg.joiner_router_rk(table, shard)
        qn = cfg.joiner_queue(table, shard)
        ex = cfg.joiner_router_exchange(table)
        return MessageMiddlewareExchange(
            host=host,
            exchange_name=ex,
            route_keys=[rk],
            consumer=True,
            queue_name=qn,
        )

    inputs[Opcodes.NEW_TRANSACTION_ITEMS] = make("transaction_items")
    inputs[Opcodes.NEW_TRANSACTION] = make("transactions")
    inputs[Opcodes.NEW_USERS] = make("users")
    inputs[Opcodes.NEW_MENU_ITEMS] = make("menu_items")
    inputs[Opcodes.NEW_STORES] = make("stores")

    return inputs


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Joiner Worker (shard-based)")
    p.add_argument(
        "-c",
        "--config",
        default=os.environ.get("CFG", "config.ini"),
        help="Ruta al config.ini",
    )
    p.add_argument(
        "--log-level",
        default=os.environ.get("LOG_LEVEL", "INFO"),
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Nivel de logging",
    )
    return p.parse_args(argv)


def resolve_config_path(cli_value: str | None) -> str:
    candidates = []
    if cli_value:
        candidates.append(cli_value)
    env_cfg_path = os.getenv("CONFIG_PATH")
    if env_cfg_path:
        candidates.append(env_cfg_path)
    env_cfg = os.getenv("CFG")
    if env_cfg:
        candidates.append(env_cfg)
    candidates.extend(("/config/config.ini", "./config.ini", "/app_config/config.ini"))

    for path in candidates:
        if path and os.path.exists(path):
            return os.path.abspath(path)

    return os.path.abspath(candidates[0])


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("-c", "--config", help="Ruta al config.ini")
    ap.add_argument("--log-level", default=os.environ.get("LOG_LEVEL", "INFO"))
    args = ap.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s [joiner-worker] %(message)s",
    )
    log = logging.getLogger("joiner-worker-main")

    cfg_path = resolve_config_path(args.config)
    if not os.path.exists(cfg_path):
        print(f"[joiner-worker] config no encontrado: {cfg_path}", file=sys.stderr)
        sys.exit(2)

    log.info(f"Usando config: {cfg_path}")

    try:
        shard = int(os.environ["JOINER_WORKER_INDEX"])
    except KeyError:
        log.error("JOINER_WORKER_INDEX no está definido en el entorno")
        sys.exit(1)
    except ValueError:
        log.error("JOINER_WORKER_INDEX debe ser un entero")
        sys.exit(1)

    try:
        cfg = Config(cfg_path)
    except Exception as e:
        log.error(f"No pude cargar config: {e}")
        sys.exit(2)

    host = cfg.broker.host
    out_q_name = cfg.names.results_controller_queue

    in_mw = build_inputs_for_shard(cfg, host, shard)
    out_results = MessageMiddlewareQueue(host=host, queue_name=out_q_name)

    worker = JoinerWorker(
        in_mw=in_mw,
        out_results_mw=out_results,
        data_dir=os.environ.get("JOINER_DATA_DIR", "/data/joiner"),
        logger=logging.getLogger(f"joiner-worker-{shard}"),
    )

    stop_event = threading.Event()

    def _handle_sig(*_):
        log.info("Recibida señal, deteniendo joiner...")
        stop_event.set()
        try:
            for mw in in_mw.values():
                mw.stop_consuming()
        except Exception:
            pass
        try:
            out_results.close()
        except Exception:
            pass
        sys.exit(0)

    signal.signal(signal.SIGINT, _handle_sig)
    signal.signal(signal.SIGTERM, _handle_sig)

    log.info(
        "Iniciando JoinerWorker shard=%d -> results=%s",
        shard,
        out_q_name,
    )
    ensure_joiner_bindings(cfg, host, shard)
    in_mw = build_inputs_for_shard(cfg, host, shard)
    worker.run()


if __name__ == "__main__":
    main()
