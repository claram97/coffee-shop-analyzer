#!/usr/bin/env python3
"""
Entry point for the Filter Worker service.
"""
import logging
import os
import signal
import sys
import threading
from dataclasses import dataclass

from worker import FilterWorker

from app_config.config_loader import Config
from leader_election import ElectionCoordinator, HeartbeatClient, FollowerRecoveryManager

HOST_HASH_MODULUS = 100
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


def _configure_logging() -> logging.Logger:
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    return logging.getLogger("filter-worker-main")


def _resolve_worker_id(logger: logging.Logger) -> int:
    worker_id_env = os.getenv("FILTER_WORKER_INDEX")
    if worker_id_env is not None:
        return int(worker_id_env)
    fallback = hash(os.getenv("HOSTNAME", "filter-worker")) % HOST_HASH_MODULUS
    logger.info("FILTER_WORKER_INDEX not set, using fallback id %s", fallback)
    return fallback


def _register_signal_handlers(stop_event: threading.Event, logger: logging.Logger) -> None:
    def shutdown_handler(*_a):
        logger.info("Shutdown signal received. Initiating graceful shutdown...")
        stop_event.set()

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)


def _load_config(logger: logging.Logger) -> Config:
    cfg_path = os.getenv("CONFIG_PATH", "./config/config.ini")
    logger.info("Starting Filter Worker...")
    logger.info("Loading config from %s", cfg_path)
    return Config(cfg_path)


def _log_worker_endpoints(cfg: Config, logger: logging.Logger) -> None:
    logger.info("Connecting to RabbitMQ at %s", cfg.broker.host)
    logger.info("Input queue (filters pool): %s", cfg.names.filters_pool_queue)
    logger.info("Output exchange (router input): %s", cfg.names.orch_to_fr_exchange)


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
    worker_id: int,
    cfg: Config,
    hb: HeartbeatSettings,
    logger: logging.Logger,
):
    election_coordinator = None
    heartbeat_client = None
    follower_recovery = None

    def handle_leader_change(new_leader_id: int, am_i_leader: bool):
        logger.info(
            "Leader update | new_leader=%s | am_i_leader=%s",
            new_leader_id,
            am_i_leader,
        )
        if heartbeat_client:
            heartbeat_client.deactivate() if am_i_leader else heartbeat_client.activate()
        if follower_recovery:
            follower_recovery.set_leader_state(am_i_leader)

    try:
        total_filter_workers = cfg.workers.filters
        worker_port_base = cfg.election_ports.filter_workers
        election_port = int(os.getenv("ELECTION_PORT", worker_port_base + worker_id))

        all_nodes = [
            (i, f"filter-worker-{i}", worker_port_base + i)
            for i in range(total_filter_workers)
        ]

        logger.info(
            "Initializing election coordinator for filter-worker-%s on port %s",
            worker_id,
            election_port,
        )
        logger.info("Cluster nodes: %s", all_nodes)

        election_coordinator = ElectionCoordinator(
            my_id=worker_id,
            my_host="0.0.0.0",
            my_port=election_port,
            all_nodes=all_nodes,
            on_leader_change=handle_leader_change,
            election_timeout=ELECTION_TIMEOUT_SECONDS,
        )

        heartbeat_client = HeartbeatClient(
            coordinator=election_coordinator,
            my_id=worker_id,
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
            my_id=worker_id,
            node_container_map=node_container_map,
            check_interval=max(MIN_FOLLOWER_CHECK_INTERVAL, hb.interval),
            down_timeout=hb.follower_down_timeout,
            restart_cooldown=hb.follower_restart_cooldown,
            startup_grace=hb.follower_recovery_grace,
            max_restart_attempts=hb.follower_max_restart_attempts,
        )

        election_coordinator.start()
        logger.info("Election listener started on port %s", election_port)
        heartbeat_client.start()
        heartbeat_client.activate()
        follower_recovery.start()

    except Exception as e:
        logger.error("Failed to initialize election coordinator: %s", e, exc_info=True)
        election_coordinator = None
        heartbeat_client = None
        follower_recovery = None

    return election_coordinator, heartbeat_client, follower_recovery


def _build_worker(cfg: Config, stop_event: threading.Event) -> FilterWorker:
    return FilterWorker(
        host=cfg.broker.host,
        in_queue=cfg.names.filters_pool_queue,
        out_router_exchange=cfg.names.orch_to_fr_exchange,
        out_router_rk_fmt=cfg.names.orch_to_fr_rk_fmt,
        num_routers=cfg.routers.filter,
        stop_event=stop_event,
    )


def _shutdown_components(
    election_coordinator,
    follower_recovery,
    heartbeat_client,
    worker,
    logger: logging.Logger,
):
    logger.info("Cleaning up resources...")
    if election_coordinator:
        election_coordinator.graceful_resign()
        logger.info("Stopping election coordinator...")
        election_coordinator.stop()
    if follower_recovery:
        follower_recovery.stop()
    if heartbeat_client:
        logger.info("Stopping heartbeat client...")
        heartbeat_client.stop()
    if worker:
        worker.shutdown()
    logger.info("Graceful shutdown complete. Exiting.")


def main():
    logger = _configure_logging()
    worker_id = _resolve_worker_id(logger)
    stop_event = threading.Event()
    _register_signal_handlers(stop_event, logger)

    cfg = _load_config(logger)
    _log_worker_endpoints(cfg, logger)

    hb_settings = _read_heartbeat_settings()
    (
        election_coordinator,
        heartbeat_client,
        follower_recovery,
    ) = _start_leader_election_components(worker_id, cfg, hb_settings, logger)

    if not (election_coordinator and heartbeat_client and follower_recovery):
        logger.critical("Leader election components failed to start, aborting.")
        sys.exit(1)

    worker = None
    try:
        worker = _build_worker(cfg, stop_event)
        worker.run()
        logger.info("Filter Worker is running. Press Ctrl+C to exit.")
        stop_event.wait()
    except Exception as e:
        logger.error("An unexpected error occurred: %s", e, exc_info=True)
    finally:
        _shutdown_components(
            election_coordinator,
            follower_recovery,
            heartbeat_client,
            worker,
            logger,
        )


if __name__ == "__main__":
    main()
