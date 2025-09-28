import os
import signal
import threading
import time
import logging
from typing import Dict, List

from middleware_client import MessageMiddlewareQueue, MessageMiddlewareDisconnectedError

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [ResultsRouter] - %(message)s'
)
logger = logging.getLogger(__name__)

def _simple_hash(data: str) -> int:
    """A conceptually simple hash function that sums the ordinal values of characters."""
    hash_value = 0
    for character in data:
        hash_value += ord(character)
    return hash_value

def _read_u8_list_from_body(body: bytes, offset: int) -> tuple[List[int], int]:
    """Helper to read a u8 list (count + items) from a byte string."""
    if offset >= len(body):
        raise ValueError("Not enough data to read list count.")
    count = body[offset]
    offset += 1
    
    if offset + count > len(body):
        raise ValueError("Not enough data to read list items.")
    
    items = list(body[offset : offset + count])
    offset += count
    return items, offset

def _extract_first_query_id(body: bytes) -> str:
    """
    Parses the DataBatch binary format to extract the first query_id.
    This is compatible with the full protocol.py definition.
    """
    try:
        # Skip the outer frame: opcode (1 byte) + length (4 bytes) = 5 bytes
        if len(body) < 5:
            raise ValueError("Message too short to contain outer frame")
            
        opcode = body[0]
        if opcode != 11:  # DATA_BATCH opcode
            raise ValueError(f"Expected DATA_BATCH opcode (11), got {opcode}")
            
        # Skip opcode and length to get to DataBatch inner content
        inner_body = body[5:]
        
        offset = 0
        table_ids, offset = _read_u8_list_from_body(inner_body, offset)
        query_ids, _ = _read_u8_list_from_body(inner_body, offset)

        if not query_ids:
            raise ValueError("Message contains no query_ids for routing.")
        
        return str(query_ids[0])

    except IndexError:
        raise ValueError("Message body is malformed or incomplete.")

class ResultsRouter:
    """
    Consumes messages and routes them to the correct ResultsFinisher
    node based on a hash of the query_id.
    """
    def __init__(self,
                 input_client: MessageMiddlewareQueue,
                 output_clients: Dict[str, MessageMiddlewareQueue]):
        
        if not output_clients:
            raise ValueError("Must provide at least one output client.")

        self.input_client = input_client
        self.output_clients = output_clients
        self.output_queue_names = sorted(output_clients.keys())
        self.finisher_count = len(self.output_queue_names)

        logging.info(f"Router initialized to distribute to {self.finisher_count} finisher queues: {self.output_queue_names}")

    def _get_target_queue(self, query_id: str) -> str:
        """Determines the target queue using a simple hash algorithm."""
        if self.finisher_count == 1:
            return self.output_queue_names[0]

        hash_as_int = _simple_hash(query_id)
        target_index = hash_as_int % self.finisher_count
        
        return self.output_queue_names[target_index]

    def _message_handler(self, body: bytes):
        """Callback for incoming messages."""
        try:
            query_id = _extract_first_query_id(body)
            target_queue_name = self._get_target_queue(query_id)
            target_client = self.output_clients[target_queue_name]
            target_client.send(body)
            logging.debug(f"Routed message for query '{query_id}' to queue '{target_queue_name}'")
        except (ValueError, UnicodeDecodeError) as e:
            logging.error(f"Failed to parse message, discarding. Error: {e}. Body prefix: {body[:50]}")
        except Exception as e:
            logging.error(f"Unexpected error in handler: {e}", exc_info=True)

    def start(self):
        """Starts the consumer in a background thread."""
        logging.info("ResultsRouter starting consumer...")
        self.input_client.start_consuming(self._message_handler)

    def stop(self):
        """Stops the consumer and closes all connections gracefully."""
        logging.info("ResultsRouter stopping...")
        self.input_client.stop_consuming()
        self.input_client.close()
        for client in self.output_clients.values():
            client.close()
        logging.info("ResultsRouter stopped.")


def main():
    """Main entry point for the ResultsRouter service."""
    try:
        rabbitmq_host = os.getenv("RABBITMQ_HOST", "localhost")
        input_queue_name = os.getenv("INPUT_QUEUE", "results_router_input_queue")
        output_queues_str = os.getenv("OUTPUT_QUEUES", "results_finisher_queue_1")
        output_queue_names = sorted([q.strip() for q in output_queues_str.split(',') if q.strip()])

        print("--- ResultsRouter Service (Class-based) ---")
        print(f"RabbitMQ Host: {rabbitmq_host}")
        print(f"Input Queue: {input_queue_name}")
        print(f"Output Queues: {output_queue_names}")
        print("-------------------------------------------")

        if not output_queue_names:
            raise ValueError("OUTPUT_QUEUES environment variable must be set.")

        input_client = MessageMiddlewareQueue(host=rabbitmq_host, queue_name=input_queue_name)
        output_clients = {
            name: MessageMiddlewareQueue(host=rabbitmq_host, queue_name=name)
            for name in output_queue_names
        }

        router = ResultsRouter(input_client=input_client, output_clients=output_clients)
        
        shutdown_event = threading.Event()
        def shutdown_handler(signum, frame):
            print("\nShutdown signal received. Stopping router...")
            router.stop()
            shutdown_event.set()
            
        signal.signal(signal.SIGINT, shutdown_handler)
        signal.signal(signal.SIGTERM, shutdown_handler)

        router.start()
        
        print("Service is running. Press Ctrl+C to exit.")
        shutdown_event.wait()
        
        print("Shutdown complete.")

    except (MessageMiddlewareDisconnectedError, ValueError) as e:
        print(f"Could not start router. Aborting. Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred during setup: {e}", exc_info=True)


if __name__ == '__main__':
    main()

