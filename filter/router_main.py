#!/usr/bin/env python3
"""
Entry point for the Filter Router service.
"""
import os
import logging
import time

from router import (
    RouterServer,
    QueueBusProducer,
    QueryPolicyResolver,
    QueueTableConfig,
)


def main():
    # Configure logging
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger = logging.getLogger("filter-router-main")
    logger.info("Starting Filter Router...")
    
    # Configuration from environment
    rabbitmq_host = os.getenv("RABBITMQ_HOST", "rabbitmq")
    router_input_queue = os.getenv("ROUTER_INPUT_QUEUE", "filter_router_queue")
    filters_pool_queue = os.getenv("FILTERS_POOL_QUEUE", "filters.pool")
    agg_queue_fmt = os.getenv("AGG_QUEUE_FORMAT", "agg.{table}.p{pid}")
    
    # Table partition configuration
    table_partitions = {
        "transactions": int(os.getenv("TRANSACTIONS_PARTITIONS", "3")),
        "users": int(os.getenv("USERS_PARTITIONS", "3")),
        "transaction_items": int(os.getenv("TRANSACTION_ITEMS_PARTITIONS", "3")),
    }
    
    logger.info(f"Connecting to RabbitMQ at {rabbitmq_host}")
    logger.info(f"Router input queue: {router_input_queue}")
    logger.info(f"Filters pool queue: {filters_pool_queue}")
    logger.info(f"Aggregator queue format: {agg_queue_fmt}")
    logger.info(f"Table partitions: {table_partitions}")
    
    try:
        # Create components
        producer = QueueBusProducer(
            host=rabbitmq_host,
            filters_pool_queue=filters_pool_queue,
            router_input_queue=router_input_queue,
            agg_queue_fmt=agg_queue_fmt,
        )
        
        policy = QueryPolicyResolver()
        table_cfg = QueueTableConfig(table_partitions)
        
        # Create and start router server
        server = RouterServer(
            host=rabbitmq_host,
            router_input_queue=router_input_queue,
            producer=producer,
            policy=policy,
            table_cfg=table_cfg,
        )
        
        logger.info("Filter Router started successfully")
        server.run()
        
        # Keep the main thread alive
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Shutting down Filter Router...")
            server.stop()
        
    except KeyboardInterrupt:
        logger.info("Shutting down Filter Router...")
        try:
            server.stop()
        except:
            pass
    except Exception as e:
        logger.error(f"Error in Filter Router: {e}")
        raise


if __name__ == "__main__":
    main()
