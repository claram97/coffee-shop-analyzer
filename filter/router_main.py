from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
import threading

from router import ExchangeBusProducer, QueryPolicyResolver, RouterServer, TableConfig

from app_config.config_loader import Config, ConfigError
from middleware.middleware_client import MessageMiddlewareExchange


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


def build_filter_router_from_config(
    cfg: Config, stop_event: threading.Event
) -> RouterServer:
    pid = int(os.environ["FILTER_ROUTER_INDEX"])
    router_in = MessageMiddlewareExchange(
        host=cfg.broker.host,
        exchange_name=cfg.names.orch_to_fr_exchange,
        route_keys=[cfg.orchestrator_rk(pid)],
        consumer=True,
        queue_name=f"filter_router_in_{pid}",
    )
    producer = ExchangeBusProducer(
        host=cfg.broker.host,
        filters_pool_queue=cfg.names.filters_pool_queue,
        in_mw=router_in,
        exchange_fmt=cfg.names.filter_router_exchange_fmt,
        rk_fmt=cfg.names.filter_router_rk_fmt,
        router_id=pid,
    )
    table_cfg = TableConfig(cfg.workers.aggregators)
    policy = QueryPolicyResolver()
    server = RouterServer(
        host=cfg.broker.host,
        router_in=router_in,
        producer=producer,
        policy=policy,
        table_cfg=table_cfg,
        stop_event=stop_event,
        orch_workers=cfg.workers.orchestrators,
    )
    return server


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-c", "--config", help="Ruta al config.ini")
    ap.add_argument("--log-level", default=os.environ.get("LOG_LEVEL", "INFO"))
    args = ap.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    log = logging.getLogger("filter-router-main")

    cfg_path = resolve_config_path(args.config)
    if not os.path.exists(cfg_path):
        print("[filter-router] config no encontrado: %s", cfg_path, file=sys.stderr)
        sys.exit(2)

    log.info("Usando config: %s", cfg_path)

    try:
        cfg = Config(cfg_path)
    except ConfigError as e:
        print("[filter-router] no pude cargar config: %s", e, file=sys.stderr)
        sys.exit(2)
    stop_event = threading.Event()

    def shutdown_handler(*_a):
        log.info("Shutdown signal received. Initiating graceful shutdown...")
        stop_event.set()

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)
    server = build_filter_router_from_config(cfg, stop_event)
    try:
        server.run()
        log.info("Filter router is running. Press Ctrl+C to exit.")
        stop_event.wait()
    finally:
        log.info("Cleaning up resources...")
        server.stop()
        log.info("Graceful shutdown complete. Exiting.")


if __name__ == "__main__":
    main()
