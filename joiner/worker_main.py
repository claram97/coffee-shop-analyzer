#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
import threading
from typing import Dict

import pika

from app_config.config_loader import Config
from joiner.worker import JoinerWorker
from middleware.middleware_client import (
    MessageMiddlewareExchange,
    MessageMiddlewareQueue,
)
from protocol2.table_data_pb2 import TableName


def force_bind(host: str, exchange: str, queue: str, routing_key: str):
    conn = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=host, heartbeat=1200, blocked_connection_timeout=600
        )
    )
    ch = conn.channel()
    ch.exchange_declare(exchange=exchange, exchange_type="topic", durable=True)
    ch.queue_declare(queue=queue, durable=True)
    ch.queue_bind(exchange=exchange, queue=queue, routing_key=routing_key)
    conn.close()


def ensure_joiner_bindings(cfg: Config, host: str, shard: int) -> None:
    """
    Declara la cola estable de cada tabla para este shard y asegura el binding
    exchange -> routing_key -> queue.
    """
    tables = ["menu_items", "stores", "transactions", "transaction_items", "users"]
    for table in tables:
        rk = cfg.joiner_router_rk(table, shard)
        qn = cfg.joiner_queue(table, shard)
        ex = cfg.joiner_router_exchange(table)

        q = MessageMiddlewareQueue(host=host, queue_name=qn)
        try:
            q.close()
        except Exception:
            pass

        _tmp_consumer = MessageMiddlewareExchange(
            host=host,
            exchange_name=ex,
            route_keys=[rk],
            consumer=True,
            queue_name=qn,
        )

        try:
            _tmp_consumer.close()
        except Exception:
            pass


def build_inputs_for_shard(
    cfg: Config, host: str, shard: int
) -> Dict[int, MessageMiddlewareExchange]:
    inputs: Dict[int, MessageMiddlewareExchange] = {}

    def make(table: str) -> MessageMiddlewareExchange:
        ex = cfg.joiner_router_exchange(table)
        rk = cfg.joiner_router_rk(table, shard)
        qn = cfg.joiner_queue(table, shard)
        return MessageMiddlewareExchange(
            host=host,
            exchange_name=ex,
            route_keys=[rk],
            consumer=True,
            queue_name=qn,
        )

    inputs[TableName.TRANSACTION_ITEMS] = make("transaction_items")
    inputs[TableName.TRANSACTIONS] = make("transactions")
    inputs[TableName.USERS] = make("users")
    inputs[TableName.MENU_ITEMS] = make("menu_items")
    inputs[TableName.STORES] = make("stores")

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


def resolve_config_path() -> str:
    candidates = []
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
    log_level = os.environ.get("LOG_LEVEL", "INFO")
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s %(levelname)s [joiner-worker] %(message)s",
    )
    log = logging.getLogger("joiner-worker-main")

    cfg_path = resolve_config_path()
    if not os.path.exists(cfg_path):
        print("[joiner-worker] config no encontrado: %s", cfg_path, file=sys.stderr)
        sys.exit(2)
    log.info("Usando config: %s", cfg_path)

    shard = int(os.environ["JOINER_WORKER_INDEX"], 0)

    try:
        cfg = Config(cfg_path)
    except Exception as e:
        log.error("No pude cargar config: %s", e)
        sys.exit(2)

    stop_event = threading.Event()

    def shutdown_handler(*_):
        log.info("Shutdown signal received. Initiating graceful shutdown...")
        stop_event.set()

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    host = cfg.broker.host
    out_q_name = cfg.names.results_controller_queue

    in_mw = build_inputs_for_shard(cfg, host, shard)

    ensure_joiner_bindings(cfg, host, shard)

    def make_results_pub():
        return MessageMiddlewareQueue(host=host, queue_name=out_q_name)

    out_results = make_results_pub()

    # Get write buffer size from config (can be overridden by environment variable)
    write_buffer_size = int(os.environ.get("JOINER_WRITE_BUFFER_SIZE", str(cfg.joiner_write_buffer_size)))
    log.info("Using write buffer size: %d", write_buffer_size)

    worker = JoinerWorker(
        in_mw=in_mw,
        out_results_mw=out_results,
        logger=logging.getLogger(f"joiner-worker-{shard}"),
        shard_index=shard,
        router_replicas=cfg.routers.joiner,
        out_factory=make_results_pub,
        stop_event=stop_event,
        write_buffer_size=write_buffer_size,
    )

    log.info(
        "Iniciando JoinerWorker shard=%d -> results=%s",
        shard,
        out_q_name,
    )

    try:
        worker.run()
        log.info("Joiner worker is running. Press Ctrl+C to exit.")
        stop_event.wait()
        log.info("Stop event received, starting shutdown process.")
    finally:
        log.info("Cleaning up resources...")
        worker.shutdown()
        log.info("Graceful shutdown complete. Exiting.")


if __name__ == "__main__":
    main()
