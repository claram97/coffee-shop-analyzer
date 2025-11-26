#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
import threading
from typing import Tuple

from app_config.config_loader import Config
from joiner.router import (
    ExchangePublisherPool,
    JoinerRouter,
    build_route_cfg_from_config,
)
from leader_election import ElectionCoordinator, HeartbeatClient, FollowerRecoveryManager
from middleware.middleware_client import (
    MessageMiddlewareExchange,
    MessageMiddlewareQueue,
)
from protocol2.table_data_pb2 import TableName


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
        self._in_queues = in_queues
        self._consumers = [MessageMiddlewareQueue(broker_host, q) for q in in_queues]
        self._threads: list[threading.Thread] = []

    def run(self, stop_evt: threading.Event):
        def mk_cb(queue_name: str):
            def callback(body: bytes, channel=None, delivery_tag=None, redelivered=False):
                return self._router._on_raw_with_ack(
                    body, channel, delivery_tag, redelivered, queue_name=queue_name
                )
            return callback

        for i, c in enumerate(self._consumers):
            queue_name = self._in_queues[i]
            t = threading.Thread(
                target=c.start_consuming, args=(mk_cb(queue_name),), daemon=False
            )
            t.start()
            self._threads.append(t)
        stop_evt.wait()

    def shutdown(self):
        """Stops all consumers and waits for their threads to finish."""
        self._logger.info("Shutting down FanInServer consumers...")
        for c in self._consumers:
            c.stop_consuming()

        self._logger.info("Waiting for consumer threads to finish...")
        for t in self._threads:
            t.join()
        self._logger.info(
            "All consumer threads finished. FanInServer shutdown complete."
        )


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


def _ensure_all_joiner_bindings(cfg: Config, host: str):
    tables = ["menu_items", "stores", "transactions", "transaction_items", "users"]
    for t in tables:
        ex = cfg.joiner_router_exchange(t)
        shards = cfg.joiner_partitions(t)
        if t in ("menu_items", "stores") and shards <= 1:
            shards = max(1, int(cfg.workers.joiners))
        for sh in range(shards):
            rk = cfg.joiner_router_rk(t, sh)
            qn = cfg.joiner_queue(t, sh)
            tmp = MessageMiddlewareExchange(
                host=host,
                exchange_name=ex,
                route_keys=[rk],
                consumer=True,
                queue_name=qn,
            )
            try:
                tmp.stop_consuming()
                tmp.close()
            except Exception:
                pass


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-c", "--config", help="Ruta al config.ini")
    ap.add_argument("--log-level", default=os.environ.get("LOG_LEVEL", "INFO"))
    args = ap.parse_args()

    jr_index = int(os.environ["JOINER_ROUTER_INDEX"])
    election_port = None

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s [joiner-router] %(message)s",
    )
    log = logging.getLogger("joiner-router-main")

    cfg_path = resolve_config_path(args.config)
    if not os.path.exists(cfg_path):
        print("[filter-router] config no encontrado: %s", cfg_path, file=sys.stderr)
        sys.exit(2)

    log.info("Usando config: %s", cfg_path)

    try:
        cfg = Config(cfg_path)
    except Exception as e:
        log.error("No pude cargar config: %s", e)
        sys.exit(2)

    stop_event = threading.Event()

    def shutdown_handler(*_a):
        log.info("Shutdown signal received. Initiating graceful shutdown...")
        stop_event.set()

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    broker_host = cfg.broker.host
    fr_replicas = cfg.routers.filter

    heartbeat_interval = float(os.getenv("HEARTBEAT_INTERVAL_SECONDS", "1.0"))
    heartbeat_timeout = float(os.getenv("HEARTBEAT_TIMEOUT_SECONDS", "1.0"))
    heartbeat_max_misses = int(os.getenv("HEARTBEAT_MAX_MISSES", "3"))
    heartbeat_startup_grace = float(os.getenv("HEARTBEAT_STARTUP_GRACE_SECONDS", "5.0"))
    heartbeat_election_cooldown = float(os.getenv("HEARTBEAT_ELECTION_COOLDOWN_SECONDS", "10.0"))
    heartbeat_cooldown_jitter = float(os.getenv("HEARTBEAT_COOLDOWN_JITTER_SECONDS", "0.5"))
    follower_down_timeout = float(os.getenv("FOLLOWER_DOWN_TIMEOUT_SECONDS", "10.0"))
    follower_restart_cooldown = float(os.getenv("FOLLOWER_RESTART_COOLDOWN_SECONDS", "30.0"))
    follower_recovery_grace = float(os.getenv("FOLLOWER_RECOVERY_GRACE_SECONDS", "6.0"))
    follower_max_restart_attempts = int(os.getenv("FOLLOWER_MAX_RESTART_ATTEMPTS", "100"))

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
        total_joiner_routers = cfg.routers.joiner
        router_port_base = cfg.election_ports.joiner_routers
        election_port = int(os.environ.get("ELECTION_PORT", router_port_base + jr_index))

        all_nodes = [
            (i, f"joiner-router-{i}", router_port_base + i) for i in range(total_joiner_routers)
        ]

        log.info(
            f"Initializing election coordinator for joiner-router-{jr_index} on port {election_port}"
        )
        log.info(f"Cluster nodes: {all_nodes}")

        election_coordinator = ElectionCoordinator(
            my_id=jr_index,
            my_host="0.0.0.0",
            my_port=election_port,
            all_nodes=all_nodes,
            on_leader_change=handle_leader_change,
            election_timeout=5.0,
        )

        heartbeat_client = HeartbeatClient(
            coordinator=election_coordinator,
            my_id=jr_index,
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
            my_id=jr_index,
            node_container_map=node_container_map,
            check_interval=max(1.0, heartbeat_interval),
            down_timeout=follower_down_timeout,
            restart_cooldown=follower_restart_cooldown,
            startup_grace=follower_recovery_grace,
            max_restart_attempts=follower_max_restart_attempts,
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

    if not (election_coordinator and heartbeat_client and follower_recovery):
        log.critical("Leader election components failed to start, aborting.")
        sys.exit(1)

    pool = ExchangePublisherPool(factory=_rabbit_exchange_factory(broker_host))

    route_cfg = build_route_cfg_from_config(cfg)

    router = JoinerRouter(
        in_mw=None,
        publisher_pool=pool,
        route_cfg=route_cfg,
        fr_replicas=fr_replicas,
        stop_event=stop_event,
        router_id=jr_index,
    )

    in_queues: list[str] = []
    tables_for_input: list[Tuple[int, str]] = [
        (TableName.TRANSACTION_ITEMS, "transaction_items"),
        (TableName.TRANSACTIONS, "transactions"),
        (TableName.USERS, "users"),
        (TableName.MENU_ITEMS, "menu_items"),
        (TableName.STORES, "stores"),
    ]
    for _tname, tnamestr in tables_for_input:
        parts = cfg.workers.aggregators
        for pid in range(parts):
            q = cfg.aggregator_to_joiner_router_queue(tnamestr, pid, jr_index)
            in_queues.append(q)

    log.info("Entradas (N=%d): %s", len(in_queues), ", ".join(in_queues))

    server = _FanInServer(broker_host, in_queues, router, log)
    try:
        _ensure_all_joiner_bindings(cfg, broker_host)
        server.run(stop_evt=stop_event)
        log.info("Joiner router is running. Press Ctrl+C to exit.")
        stop_event.wait()
        log.info("Stop event received, starting shutdown process.")
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

        server.shutdown()
        router.shutdown()
        log.info("Graceful shutdown complete. Exiting.")


if __name__ == "__main__":
    main()
