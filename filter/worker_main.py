#!/usr/bin/env python3
"""
Entry point for the Filter Worker service.
"""
import os
import logging
import time

from worker import FilterWorker


def main():
    # Configure logging
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger = logging.getLogger("filter-worker-main")
    logger.info("Starting Filter Worker...")
    
    # Configuration from environment
    rabbitmq_host = os.getenv("RABBITMQ_HOST", "rabbitmq")
    filters_pool_queue = os.getenv("FILTERS_POOL_QUEUE", "filters.pool")
    router_output_queue = os.getenv("ROUTER_OUTPUT_QUEUE", "filter_router_queue")
    
    logger.info(f"Connecting to RabbitMQ at {rabbitmq_host}")
    logger.info(f"Input queue (filters pool): {filters_pool_queue}")
    logger.info(f"Output queue (router input): {router_output_queue}")
    
    try:
        # Create and start filter worker
        worker = FilterWorker(
            host=rabbitmq_host,
            in_queue=filters_pool_queue,
            out_router_queue=router_output_queue,
        )
        
        logger.info("Filter Worker started successfully")
        worker.run()
        
        # Keep the main thread alive
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
