#!/usr/bin/env python3
"""
Entry point for the Filter Worker service.
"""
import logging
import os
import time

from worker import FilterWorker

from app_config.config_loader import Config


def main():
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, log_level),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    logger = logging.getLogger("filter-worker-main")
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

    try:
        worker = FilterWorker(
            host=rabbitmq_host,
            in_queue=filters_pool_queue,
            out_router_exchange=router_exchange,
            out_router_rk_fmt=router_rk_fmt,
            num_routers=num_routers,
        )

        logger.info("Filter Worker started successfully")
        worker.run()

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Shutting down Filter Worker...")

    except KeyboardInterrupt:
        logger.info("Shutting down Filter Worker...")
    except Exception as e:
        logger.error(f"Error in Filter Worker: {e}")
        raise


if __name__ == "__main__":
    main()
