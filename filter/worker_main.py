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
from leader_election import ElectionCoordinator


def main():
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, log_level),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger = logging.getLogger("filter-worker-main")

    # Filter workers don't have an explicit index, but we can use a unique identifier
    # Since they're in a pool, we'll use the hostname or generate an ID
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

    # Initialize election coordinator (listener only for now)
    election_coordinator = None
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
            on_leader_change=None,
            election_timeout=5.0
        )
        
        election_coordinator.start()
        logger.info(f"Election listener started on port {election_port}")
        
    except Exception as e:
        logger.error(f"Failed to initialize election coordinator: {e}", exc_info=True)
        election_coordinator = None

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
            logger.info("Stopping election coordinator...")
            election_coordinator.stop()
        
        if worker:
            worker.shutdown()
        logger.info("Graceful shutdown complete. Exiting.")


if __name__ == "__main__":
    main()
