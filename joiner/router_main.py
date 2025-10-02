#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
import threading
from typing import Dict, Tuple

from app_config.config_loader import Config
from joiner.router import ExchangePublisherPool, JoinerRouter, TableRouteCfg
from middleware.middleware_client import (
    MessageMiddlewareExchange,
    MessageMiddlewareQueue,
)
from protocol import Opcodes


def _build_route_cfg(cfg: Config) -> Dict[int, TableRouteCfg]:
    """
    Crea el mapping table_id -> TableRouteCfg usando:
      - joiner_router_exchange_fmt
      - joiner_router_rk_fmt (solo para chequear/dep; el patrón per-table lo arma el Router)
      - agg_shards (cuántas particiones salen del Aggregator por tabla)
      - joiner_shards (cuántos workers/joiner shards recibe esa tabla)
    """

    def ex(table: str) -> str:
        return cfg.joiner_router_exchange(table)

    def key_pattern_for_table(table: str) -> str:
        pat = cfg.names.joiner_router_rk_fmt
        return pat.replace("{table}", table)

    tables: Dict[int, str] = {
        Opcodes.NEW_TRANSACTION: "transactions",
        Opcodes.NEW_TRANSACTION_ITEMS: "transaction_items",
        Opcodes.NEW_USERS: "users",
        Opcodes.NEW_MENU_ITEMS: "menu_items",
        Opcodes.NEW_STORES: "stores",
    }

    route: Dict[int, TableRouteCfg] = {}
    for tid, tname in tables.items():
        route[tid] = TableRouteCfg(
            exchange_name=ex(tname),
            agg_shards=cfg.agg_partitions(tname),
            joiner_shards=cfg.joiner_partitions(tname),
            key_pattern=key_pattern_for_table(tname),
        )
    return route


def _rabbit_exchange_factory(host: str):
    def factory(exchange_name: str, routing_key: str) -> MessageMiddlewareExchange:
        return MessageMiddlewareExchange(
            host=host, exchange_name=exchange_name, route_keys=[routing_key]
        )

    return factory


class _FanInServer:
    """
    Arranca N consumidores (uno por cola de entrada) y despacha todos los mensajes al JoinerRouter._on_raw.
    """

    def __init__(
        self,
        broker_host: str,
        in_queues: list[str],
        router: JoinerRouter,
        logger: logging.Logger,
    ):
        self._logger = logger
        self._router = router
        self._consumers = [MessageMiddlewareQueue(broker_host, q) for q in in_queues]
        self._threads: list[threading.Thread] = []

    def run(self):
        def mk_cb():
            return lambda body: self._router._on_raw(body)

        for c in self._consumers:
            t = threading.Thread(target=c.start_consuming, args=(mk_cb(),), daemon=True)
            t.start()
            self._threads.append(t)

        stop_evt = threading.Event()

        def _stop(*_a):
            stop_evt.set()

        signal.signal(signal.SIGINT, _stop)
        signal.signal(signal.SIGTERM, _stop)
        stop_evt.wait()


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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-c", "--config", help="Ruta al config.ini")
    ap.add_argument("--log-level", default=os.environ.get("LOG_LEVEL", "INFO"))
    args = ap.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s [joiner-router] %(message)s",
    )
    log = logging.getLogger("joiner-router-main")

    cfg_path = resolve_config_path(args.config)
    if not os.path.exists(cfg_path):
        print(f"[filter-router] config no encontrado: {cfg_path}", file=sys.stderr)
        sys.exit(2)

    log.info(f"Usando config: {cfg_path}")

    try:
        cfg = Config(cfg_path)
    except Exception as e:
        log.error(f"No pude cargar config: {e}")
        sys.exit(2)

    broker_host = cfg.broker.host

    pool = ExchangePublisherPool(factory=_rabbit_exchange_factory(broker_host))

    route_cfg = _build_route_cfg(cfg)

    router = JoinerRouter(
        in_mw=None,
        publisher_pool=pool,
        route_cfg=route_cfg,
    )

    in_queues: list[str] = []
    tables_for_input: list[Tuple[int, str]] = [
        (Opcodes.NEW_TRANSACTION_ITEMS, "transaction_items"),
        (Opcodes.NEW_TRANSACTION, "transactions"),
        (Opcodes.NEW_USERS, "users"),
        (Opcodes.NEW_MENU_ITEMS, "menu_items"),
        (Opcodes.NEW_STORES, "stores"),
    ]
    for tid, tname in tables_for_input:
        parts = cfg.agg_partitions(tname)
        for pid in range(parts):
            q = cfg.aggregator_to_joiner_router_queue(tname, pid)
            in_queues.append(q)

    log.info("Entradas (N=%d): %s", len(in_queues), ", ".join(in_queues))

    server = _FanInServer(broker_host, in_queues, router, log)
    server.run()


if __name__ == "__main__":
    main()
