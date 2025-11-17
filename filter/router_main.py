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
from leader_election import ElectionCoordinator, HeartbeatClient, FollowerRecoveryManager


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

    fr_index = int(os.environ["FILTER_ROUTER_INDEX"])
    election_port = int(os.environ.get("ELECTION_PORT", 9200 + fr_index))

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
    
    heartbeat_interval = float(os.getenv("HEARTBEAT_INTERVAL_SECONDS", "1.0"))
    heartbeat_timeout = float(os.getenv("HEARTBEAT_TIMEOUT_SECONDS", "1.0"))
    heartbeat_max_misses = int(os.getenv("HEARTBEAT_MAX_MISSES", "3"))
    heartbeat_startup_grace = float(os.getenv("HEARTBEAT_STARTUP_GRACE_SECONDS", "5.0"))
    heartbeat_election_cooldown = float(os.getenv("HEARTBEAT_ELECTION_COOLDOWN_SECONDS", "10.0"))
    heartbeat_cooldown_jitter = float(os.getenv("HEARTBEAT_COOLDOWN_JITTER_SECONDS", "0.5"))
    follower_down_timeout = float(os.getenv("FOLLOWER_DOWN_TIMEOUT_SECONDS", "10.0"))
    follower_restart_cooldown = float(os.getenv("FOLLOWER_RESTART_COOLDOWN_SECONDS", "30.0"))
    follower_recovery_grace = float(os.getenv("FOLLOWER_RECOVERY_GRACE_SECONDS", "6.0"))

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
            if am_i_leader:
                heartbeat_client.deactivate()
            else:
                heartbeat_client.activate()
        if follower_recovery:
            follower_recovery.set_leader_state(am_i_leader)
    try:
        total_filter_routers = cfg.routers.filter
        
        # Build list of all filter router nodes in the cluster
        all_nodes = [
            (i, f"filter-router-{i}", 9200 + i)
            for i in range(total_filter_routers)
        ]
        
        log.info(f"Initializing election coordinator for filter-router-{fr_index} on port {election_port}")
        log.info(f"Cluster nodes: {all_nodes}")
        
        election_coordinator = ElectionCoordinator(
            my_id=fr_index,
            my_host="0.0.0.0",
            my_port=election_port,
            all_nodes=all_nodes,
            on_leader_change=handle_leader_change,
            election_timeout=5.0
        )
        
        heartbeat_client = HeartbeatClient(
            coordinator=election_coordinator,
            my_id=fr_index,
            all_nodes=all_nodes,
            heartbeat_interval=heartbeat_interval,
            heartbeat_timeout=heartbeat_timeout,
            max_missed_heartbeats=heartbeat_max_misses,
            startup_grace=heartbeat_startup_grace,
            election_cooldown=heartbeat_election_cooldown,
            cooldown_jitter=heartbeat_cooldown_jitter,
        )

        node_container_map = {node_id: name for node_id, name, _ in all_nodes}
        follower_recovery = FollowerRecoveryManager(
            coordinator=election_coordinator,
            my_id=fr_index,
            node_container_map=node_container_map,
            check_interval=max(1.0, heartbeat_interval),
            down_timeout=follower_down_timeout,
            restart_cooldown=follower_restart_cooldown,
            startup_grace=follower_recovery_grace,
        )

        election_coordinator.start()
        log.info(f"Election listener started on port {election_port}")
        heartbeat_client.start()
        heartbeat_client.activate()
        follower_recovery.start()
        
    except Exception as e:
        log.error(f"Failed to initialize election coordinator: {e}", exc_info=True)
        election_coordinator = None
        heartbeat_client = None
        follower_recovery = None
    
    server = build_filter_router_from_config(cfg, stop_event)
    try:
        server.run()
        log.info("Filter router is running. Press Ctrl+C to exit.")
        stop_event.wait()
    finally:
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
        
        server.stop()
        log.info("Graceful shutdown complete. Exiting.")


if __name__ == "__main__":
    main()
