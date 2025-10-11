from __future__ import annotations

import argparse
import logging
import os
import sys
import time

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


def build_filter_router_from_config(cfg: Config) -> RouterServer:
    pid = int(os.environ["FILTER_ROUTER_INDEX"])
    router_in = MessageMiddlewareExchange(
        host=cfg.broker.host,
        exchange_name=cfg.names.orch_to_fr_exchange,
        route_keys=[cfg.orchestrator_rk(pid)],
    )
    producer = ExchangeBusProducer(
        host=cfg.broker.host,
        filters_pool_queue=cfg.names.filters_pool_queue,
        in_mw=router_in,
        exchange_fmt=cfg.names.filter_router_exchange_fmt,
        rk_fmt=cfg.names.filter_router_rk_fmt,
    )
    table_cfg = TableConfig(cfg.workers.aggregators)
    policy = QueryPolicyResolver()
    server = RouterServer(
        host=cfg.broker.host,
        router_in=router_in,
        producer=producer,
        policy=policy,
        table_cfg=table_cfg,
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
        print(f"[filter-router] config no encontrado: {cfg_path}", file=sys.stderr)
        sys.exit(2)

    log.info(f"Usando config: {cfg_path}")

    try:
        cfg = Config(cfg_path)
    except ConfigError as e:
        print(f"[filter-router] no pude cargar config: {e}", file=sys.stderr)
        sys.exit(2)

    server = build_filter_router_from_config(cfg)
    server.run()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
