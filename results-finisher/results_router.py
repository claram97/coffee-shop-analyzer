import logging
import os
import signal
import threading
from typing import Dict, List

from middleware.middleware_client import MessageMiddlewareQueue, MessageMiddlewareDisconnectedError
from protocol import DataBatch, ProtocolError, Opcodes

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(name)s] - %(message)s'
)
logger = logging.getLogger(__name__)

def _calculate_route_hash(data: str) -> int:
    """
    A simple, deterministic hash function that sums the ordinal values of characters.
    """
    return sum(ord(char) for char in data)

class ResultsRouter:
    """
    Consumes DataBatch messages and routes them to the correct ResultsFinisher
    instance based on a hash of the query_id.
    
    This ensures that all batches for a single query are processed by the same worker.
    """
    def __init__(self,
                 input_client: MessageMiddlewareQueue,
                 output_clients: Dict[str, MessageMiddlewareQueue]):
        
        if not output_clients:
            raise ValueError("Must provide at least one output client for routing.")

        self.input_client = input_client
        self.output_clients = output_clients

        self.output_queue_names = sorted(output_clients.keys())
        self.finisher_count = len(self.output_queue_names)

        logger.info(
            f"Router initialized. Distributing to {self.finisher_count} finisher queues: "
            f"{self.output_queue_names}"
        )

    def _get_target_queue_name(self, query_id: str) -> str:
        """Determines the target queue using a deterministic hash algorithm."""
        hash_value = _calculate_route_hash(query_id)
        target_index = hash_value % self.finisher_count
        return self.output_queue_names[target_index]

    def _process_message(self, body: bytes):
        """
        Parses, validates, and routes a single incoming message.
        This is the core callback for the message consumer.
        """
        if not body:
            logger.warning("Received an empty message body. Discarding.")
            return

        try:
            # The primary responsibility is to route DataBatch messages.
            if body[0] != Opcodes.DATA_BATCH:
                logger.warning(f"Received message with unsupported opcode {body[0]}. Discarding.")
                return

            message = DataBatch.deserialize_from_bytes(body)
            
            if not message.query_ids:
                logger.warning("DataBatch contains no query_ids for routing. Discarding.")
                return
            
            # Route based on the first query_id in the list.
            query_id = str(message.query_ids[0])
            batch_number = message.batch_number
            target_queue_name = self._get_target_queue_name(query_id)
            target_client = self.output_clients[target_queue_name]
            
            target_client.send(body)
            logger.info(f"Routed DATA_BATCH {batch_number} for query '{query_id}' to queue '{target_queue_name}'")

        except ProtocolError as e:
            logger.error(f"Failed to parse message due to protocol error, discarding. Error: {e}. Body prefix: {body[:60]!r}")
        except Exception as e:
            logger.critical(f"An unexpected error occurred in the message handler: {e}", exc_info=True)

    def start(self):
        """Starts the message consumer in a dedicated thread."""
        logger.info("ResultsRouter is starting...")
        self.input_client.start_consuming(self._process_message)

    def stop(self):
        """Stops the consumer and closes all connections gracefully."""
        logger.info("ResultsRouter is shutting down...")
        self.input_client.stop_consuming()
        self.input_client.close()
        for client in self.output_clients.values():
            client.close()
        logger.info("ResultsRouter has stopped.")

# --- Service Entrypoint ---
def main():
    """Main entry point for the ResultsRouter service."""
    try:
        # Load configuration from environment variables with sensible defaults.
        rabbitmq_host = os.getenv("RABBITMQ_HOST", "localhost")
        input_queue = os.getenv("INPUT_QUEUE", "results.controller.in")
        output_queues_csv = os.getenv("OUTPUT_QUEUES", "finisher_input_queue_1,finisher_input_queue_2")
        output_queues = sorted([q.strip() for q in output_queues_csv.split(',') if q.strip()])

        if not output_queues:
            raise ValueError("OUTPUT_QUEUES environment variable must be configured with at least one queue.")

        logger.info("--- ResultsRouter Service ---")
        logger.info(f"Connecting to RabbitMQ at: {rabbitmq_host}")
        logger.info(f"Consuming from input queue: {input_queue}")
        logger.info(f"Routing to output queues: {output_queues}")
        logger.info("-----------------------------")

        # Initialize middleware clients
        input_client = MessageMiddlewareQueue(host=rabbitmq_host, queue_name=input_queue)
        output_clients = {
            name: MessageMiddlewareQueue(host=rabbitmq_host, queue_name=name)
            for name in output_queues
        }

        # Setup and run the router
        router = ResultsRouter(input_client=input_client, output_clients=output_clients)
        shutdown_event = threading.Event()

        def shutdown_handler(signum, frame):
            logger.info("Shutdown signal received. Stopping router gracefully.")
            shutdown_event.set()
            
        signal.signal(signal.SIGINT, shutdown_handler)
        signal.signal(signal.SIGTERM, shutdown_handler)

        router.start()
        logger.info("Service is running. Press Ctrl+C to exit.")
        
        # Wait until shutdown is signaled
        shutdown_event.wait()
        
        # Perform cleanup
        router.stop()
        logger.info("Shutdown complete.")

    except (ValueError, MessageMiddlewareDisconnectedError) as e:
        logger.critical(f"Service could not start due to a configuration or connection error: {e}")
    except Exception as e:
        logger.critical(f"An unexpected error occurred during service setup: {e}", exc_info=True)

if __name__ == '__main__':
    main()