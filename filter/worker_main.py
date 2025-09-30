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
    # Configure logging
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, log_level),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    logger = logging.getLogger("filter-worker-main")
    logger.info("Starting Filter Worker...")

    # Cargar configuración desde archivo ini
    cfg_path = os.getenv("CONFIG_PATH", "./config/config.ini")
    cfg = Config(cfg_path)

    # Datos de broker y colas desde config.ini
    rabbitmq_host = cfg.broker.host
    filters_pool_queue = cfg.names.filters_pool_queue
    router_output_queue = cfg.names.orch_to_fr_queue

    logger.info(f"Connecting to RabbitMQ at {rabbitmq_host}")
    logger.info(f"Input queue (filters pool): {filters_pool_queue}")
    logger.info(f"Output queue (router input): {router_output_queue}")

    try:
        # Crear y arrancar el worker
        worker = FilterWorker(
            host=rabbitmq_host,
            in_queue=filters_pool_queue,
            out_router_queue=router_output_queue,
        )

        logger.info("Filter Worker started successfully")
        worker.run()

        # Mantener vivo el proceso principal
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
