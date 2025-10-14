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


def main():
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, log_level),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger = logging.getLogger("filter-worker-main")

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
        if worker:
            worker.shutdown()
        logger.info("Graceful shutdown complete. Exiting.")


if __name__ == "__main__":
    main()
