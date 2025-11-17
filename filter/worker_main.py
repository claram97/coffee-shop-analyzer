#!/usr/bin/env python3
"""
Entry point for the Filter Worker service.
"""
import logging
import os
import signal
import threading

from worker import FilterWorker

from app_config.config_loader import Config
from leader_election import ElectionCoordinator, HeartbeatClient, FollowerRecoveryManager


def main():
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, log_level),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger = logging.getLogger("filter-worker-main")

    worker_id_env = os.getenv("FILTER_WORKER_INDEX")
    if worker_id_env is not None:
        worker_id = int(worker_id_env)
    else:
        # Fallback to a deterministic value based on hostname for ad-hoc runs
        worker_id = hash(os.getenv("HOSTNAME", "filter-worker")) % 100
    election_port = int(os.getenv("ELECTION_PORT", 9100 + worker_id))

    stop_event = threading.Event()

    def shutdown_handler(*_a):
        logger.info("Shutdown signal received. Initiating graceful shutdown...")
        stop_event.set()

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    logger.info("Starting Filter Worker...")
    cfg_path = os.getenv("CONFIG_PATH", "./config/config.ini")
    cfg = Config(cfg_path)

    rabbitmq_host = cfg.broker.host
    filters_pool_queue = cfg.names.filters_pool_queue
    router_exchange = cfg.names.orch_to_fr_exchange
    router_rk_fmt = cfg.names.orch_to_fr_rk_fmt
    num_routers = cfg.routers.filter

    logger.info(f"Connecting to RabbitMQ at {rabbitmq_host}")
    logger.info(f"Input queue (filters pool): {filters_pool_queue}")
    logger.info(f"Output exchange (router input): {router_exchange}")

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
        logger.info(
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
        total_filter_workers = cfg.workers.filters
        
        # Build list of all filter worker nodes in the cluster
        all_nodes = [
            (i, f"filter-worker-{i}", 9100 + i)
            for i in range(total_filter_workers)
        ]
        
        logger.info(f"Initializing election coordinator for filter-worker-{worker_id} on port {election_port}")
        logger.info(f"Cluster nodes: {all_nodes}")
        
        election_coordinator = ElectionCoordinator(
            my_id=worker_id,
            my_host="0.0.0.0",
            my_port=election_port,
            all_nodes=all_nodes,
            on_leader_change=handle_leader_change,
            election_timeout=5.0
        )

        heartbeat_client = HeartbeatClient(
            coordinator=election_coordinator,
            my_id=worker_id,
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
            my_id=worker_id,
            node_container_map=node_container_map,
            check_interval=max(1.0, heartbeat_interval),
            down_timeout=follower_down_timeout,
            restart_cooldown=follower_restart_cooldown,
            startup_grace=follower_recovery_grace,
        )
        
        election_coordinator.start()
        logger.info(f"Election listener started on port {election_port}")
        heartbeat_client.start()
        heartbeat_client.activate()
        follower_recovery.start()
        
    except Exception as e:
        logger.error(f"Failed to initialize election coordinator: {e}", exc_info=True)
        election_coordinator = None
        heartbeat_client = None
        follower_recovery = None

    worker = None
    try:
        worker = FilterWorker(
            host=rabbitmq_host,
            in_queue=filters_pool_queue,
            out_router_exchange=router_exchange,
            out_router_rk_fmt=router_rk_fmt,
            num_routers=num_routers,
            stop_event=stop_event,
        )

        worker.run()
        logger.info("Filter Worker is running. Press Ctrl+C to exit.")
        stop_event.wait()

    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}", exc_info=True)
    finally:
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


if __name__ == "__main__":
    main()
