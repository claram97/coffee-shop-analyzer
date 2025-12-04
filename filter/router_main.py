from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
import threading
from dataclasses import dataclass

from router import ExchangeBusProducer, QueryPolicyResolver, RouterServer, TableConfig

from app_config.config_loader import Config, ConfigError
from middleware.middleware_client import MessageMiddlewareExchange
from leader_election import ElectionCoordinator, HeartbeatClient, FollowerRecoveryManager

DEFAULT_HEARTBEAT_INTERVAL = 1.0
DEFAULT_HEARTBEAT_TIMEOUT = 1.0
DEFAULT_HEARTBEAT_MAX_MISSES = 3
DEFAULT_HEARTBEAT_STARTUP_GRACE = 5.0
DEFAULT_HEARTBEAT_ELECTION_COOLDOWN = 10.0
DEFAULT_HEARTBEAT_COOLDOWN_JITTER = 0.5
DEFAULT_FOLLOWER_DOWN_TIMEOUT = 10.0
DEFAULT_FOLLOWER_RESTART_COOLDOWN = 30.0
DEFAULT_FOLLOWER_RECOVERY_GRACE = 6.0
DEFAULT_FOLLOWER_MAX_RESTART_ATTEMPTS = 100
MIN_FOLLOWER_CHECK_INTERVAL = 1.0
ELECTION_TIMEOUT_SECONDS = 5.0


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
    cfg: Config, stop_event: threading.Event, router_id: int
) -> RouterServer:
    router_in = MessageMiddlewareExchange(
        host=cfg.broker.host,
        exchange_name=cfg.names.orch_to_fr_exchange,
        route_keys=[cfg.orchestrator_rk(router_id)],
        consumer=True,
        queue_name=f"filter_router_in_{router_id}",
    )
    producer = ExchangeBusProducer(
        host=cfg.broker.host,
        filters_pool_queue=cfg.names.filters_pool_queue,
        in_mw=router_in,
        exchange_fmt=cfg.names.filter_router_exchange_fmt,
        rk_fmt=cfg.names.filter_router_rk_fmt,
        router_id=router_id,
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
        router_id=router_id,
    )
    return server


@dataclass
class HeartbeatSettings:
    interval: float
    timeout: float
    max_misses: int
    startup_grace: float
    election_cooldown: float
    cooldown_jitter: float
    follower_down_timeout: float
    follower_restart_cooldown: float
    follower_recovery_grace: float
    follower_max_restart_attempts: int


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", help="Ruta al config.ini")
    parser.add_argument("--log-level", default=os.environ.get("LOG_LEVEL", "INFO"))
    return parser.parse_args()


def _configure_logging(log_level: str) -> logging.Logger:
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    return logging.getLogger("filter-router-main")


def _load_config_or_exit(config_flag: str | None, log: logging.Logger) -> Config:
    cfg_path = resolve_config_path(config_flag)
    if not os.path.exists(cfg_path):
        print("[filter-router] config no encontrado: %s" % cfg_path, file=sys.stderr)
        sys.exit(2)

    log.info("Usando config: %s", cfg_path)

    try:
        return Config(cfg_path)
    except ConfigError as e:
        print("[filter-router] no pude cargar config: %s" % e, file=sys.stderr)
        sys.exit(2)


def _register_signal_handlers(stop_event: threading.Event, log: logging.Logger) -> None:
    def shutdown_handler(*_a):
        log.info("Shutdown signal received. Initiating graceful shutdown...")
        stop_event.set()

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)


def _read_heartbeat_settings() -> HeartbeatSettings:
    return HeartbeatSettings(
        interval=float(os.getenv("HEARTBEAT_INTERVAL_SECONDS") or DEFAULT_HEARTBEAT_INTERVAL),
        timeout=float(os.getenv("HEARTBEAT_TIMEOUT_SECONDS") or DEFAULT_HEARTBEAT_TIMEOUT),
        max_misses=int(os.getenv("HEARTBEAT_MAX_MISSES") or DEFAULT_HEARTBEAT_MAX_MISSES),
        startup_grace=float(
            os.getenv("HEARTBEAT_STARTUP_GRACE_SECONDS") or DEFAULT_HEARTBEAT_STARTUP_GRACE
        ),
        election_cooldown=float(
            os.getenv("HEARTBEAT_ELECTION_COOLDOWN_SECONDS")
            or DEFAULT_HEARTBEAT_ELECTION_COOLDOWN
        ),
        cooldown_jitter=float(
            os.getenv("HEARTBEAT_COOLDOWN_JITTER_SECONDS")
            or DEFAULT_HEARTBEAT_COOLDOWN_JITTER
        ),
        follower_down_timeout=float(
            os.getenv("FOLLOWER_DOWN_TIMEOUT_SECONDS") or DEFAULT_FOLLOWER_DOWN_TIMEOUT
        ),
        follower_restart_cooldown=float(
            os.getenv("FOLLOWER_RESTART_COOLDOWN_SECONDS")
            or DEFAULT_FOLLOWER_RESTART_COOLDOWN
        ),
        follower_recovery_grace=float(
            os.getenv("FOLLOWER_RECOVERY_GRACE_SECONDS")
            or DEFAULT_FOLLOWER_RECOVERY_GRACE
        ),
        follower_max_restart_attempts=int(
            os.getenv("FOLLOWER_MAX_RESTART_ATTEMPTS")
            or DEFAULT_FOLLOWER_MAX_RESTART_ATTEMPTS
        ),
    )


def _start_leader_election_components(
    fr_index: int,
    cfg: Config,
    hb: HeartbeatSettings,
    log: logging.Logger,
) -> tuple[
    ElectionCoordinator | None,
    HeartbeatClient | None,
    FollowerRecoveryManager | None,
]:
    election_coordinator = None
    heartbeat_client = None
    follower_recovery = None

    def handle_leader_change(new_leader_id: int, am_i_leader: bool):
        log.info(
            "Leader update | new_leader=%s | am_i_leader=%s",
            new_leader_id,
            am_i_leader,
        )
        if heartbeat_client:
            heartbeat_client.deactivate() if am_i_leader else heartbeat_client.activate()
        if follower_recovery:
            follower_recovery.set_leader_state(am_i_leader)

    try:
        total_filter_routers = cfg.routers.filter
        router_port_base = cfg.election_ports.filter_routers
        election_port = int(os.environ.get("ELECTION_PORT", router_port_base + fr_index))

        all_nodes = [
            (i, f"filter-router-{i}", router_port_base + i)
            for i in range(total_filter_routers)
        ]

        log.info(
            "Initializing election coordinator for filter-router-%s on port %s",
            fr_index,
            election_port,
        )
        log.info("Cluster nodes: %s", all_nodes)

        election_coordinator = ElectionCoordinator(
            my_id=fr_index,
            my_host="0.0.0.0",
            my_port=election_port,
            all_nodes=all_nodes,
            on_leader_change=handle_leader_change,
            election_timeout=ELECTION_TIMEOUT_SECONDS,
        )

        heartbeat_client = HeartbeatClient(
            coordinator=election_coordinator,
            my_id=fr_index,
            all_nodes=all_nodes,
            heartbeat_interval=hb.interval,
            heartbeat_timeout=hb.timeout,
            max_missed_heartbeats=hb.max_misses,
            startup_grace=hb.startup_grace,
            election_cooldown=hb.election_cooldown,
            cooldown_jitter=hb.cooldown_jitter,
        )

        node_container_map = {node_id: name for node_id, name, _ in all_nodes}
        follower_recovery = FollowerRecoveryManager(
            coordinator=election_coordinator,
            my_id=fr_index,
            node_container_map=node_container_map,
            check_interval=max(MIN_FOLLOWER_CHECK_INTERVAL, hb.interval),
            down_timeout=hb.follower_down_timeout,
            restart_cooldown=hb.follower_restart_cooldown,
            startup_grace=hb.follower_recovery_grace,
            max_restart_attempts=hb.follower_max_restart_attempts,
        )

        election_coordinator.start()
        log.info("Election listener started on port %s", election_port)
        heartbeat_client.start()
        heartbeat_client.activate()
        follower_recovery.start()

    except Exception as e:
        log.error("Failed to initialize election coordinator: %s", e, exc_info=True)
        election_coordinator = None
        heartbeat_client = None
        follower_recovery = None

    return election_coordinator, heartbeat_client, follower_recovery


def _shutdown_components(
    election_coordinator,
    follower_recovery,
    heartbeat_client,
    server,
    log: logging.Logger,
):
    log.info("Cleaning up resources...")
    if election_coordinator:
        election_coordinator.graceful_resign()
        log.info("Stopping election coordinator...")
        election_coordinator.stop()
    if follower_recovery:
        follower_recovery.stop()
    if heartbeat_client:
        log.info("Stopping heartbeat client...")
        heartbeat_client.stop()
    if server:
        server.stop()
    log.info("Graceful shutdown complete. Exiting.")


def main():
    args = _parse_args()
    log = _configure_logging(args.log_level)
    fr_index = int(os.environ["FILTER_ROUTER_INDEX"])

    cfg = _load_config_or_exit(args.config, log)
    stop_event = threading.Event()
    _register_signal_handlers(stop_event, log)

    hb_settings = _read_heartbeat_settings()
    (
        election_coordinator,
        heartbeat_client,
        follower_recovery,
    ) = _start_leader_election_components(fr_index, cfg, hb_settings, log)

    if not (election_coordinator and heartbeat_client and follower_recovery):
        log.critical("Leader election components failed to start, aborting.")
        sys.exit(1)

    server = build_filter_router_from_config(cfg, stop_event, fr_index)
    try:
        server.run()
        log.info("Filter router is running. Press Ctrl+C to exit.")
        stop_event.wait()
    finally:
        _shutdown_components(
            election_coordinator,
            follower_recovery,
            heartbeat_client,
            server,
            log,
        )


if __name__ == "__main__":
    main()
