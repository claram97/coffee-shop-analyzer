import logging
import os
import signal
import threading
from typing import Dict

from google.protobuf.message import DecodeError

from middleware.middleware_client import (
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareQueue,
)
from protocol2.envelope_pb2 import Envelope, MessageType
from leader_election import ElectionCoordinator, HeartbeatClient, FollowerRecoveryManager

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

    def _get_target_queue_name(self, routing_key: str) -> str:
        """Determines the target queue using a deterministic hash algorithm."""
        hash_value = _calculate_route_hash(routing_key)
        target_index = hash_value % self.finisher_count
        return self.output_queue_names[target_index]

    def _process_message(self, body: bytes):
        # logger.info("Processing message")
        """
        Parses, validates, and routes a single incoming message.
        This is the core callback for the message consumer.
        """
        if not body:
            logger.warning("Received an empty message body. Discarding.")
            return

        try:
            # The primary responsibility is to route DataBatch messages.
            envelope = Envelope()
            envelope.ParseFromString(body)

            if envelope.type != MessageType.DATA_BATCH:
                logger.warning(
                    "Received envelope with unsupported type %s. Discarding.",
                    envelope.type,
                )
                return

            message = envelope.data_batch

            if not message.query_ids:
                logger.warning("DataBatch contains no query_ids for routing. Discarding.")
                return

            # Route based on the first query_id in the list.
            query_id = str(int(message.query_ids[0]))
            client_id = getattr(message, "client_id", None)
            if not client_id:
                logger.warning(
                    "DataBatch missing client_id for routing. Query: %s. Discarding.",
                    query_id,
                )
                return

            routing_key = f"{client_id}:{query_id}"
            batch_number = getattr(message.payload, "batch_number", 0)
            target_queue_name = self._get_target_queue_name(routing_key)
            target_client = self.output_clients[target_queue_name]
            
            target_client.send(body)
            logger.debug(
                "Routed DATA_BATCH %s for query '%s' (client %s) to queue '%s'",
                batch_number,
                query_id,
                client_id,
                target_queue_name,
            )

        except DecodeError as e:
            logger.error(
                "Failed to parse envelope due to protobuf error, discarding. Error: %s. Body prefix: %r",
                e,
                body[:60],
            )
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

        router_index_env = os.getenv("RESULTS_ROUTER_INDEX")
        if router_index_env is not None:
            router_index = int(router_index_env)
        else:
            router_index = hash(os.getenv("HOSTNAME", "results-router")) % 100
        election_port = int(os.getenv("ELECTION_PORT", 9700 + router_index))

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
            from app_config.config_loader import Config
            cfg_path = os.getenv("CONFIG_PATH", "/config/config.ini")
            cfg = Config(cfg_path)
            total_results_routers = cfg.routers.results
            
            # Build list of all results router nodes in the cluster
            all_nodes = [
                (i, f"results-router-{i}", 9700 + i)
                for i in range(total_results_routers)
            ]
            
            logger.info(f"Initializing election coordinator for results-router-{router_index} on port {election_port}")
            logger.info(f"Cluster nodes: {all_nodes}")
            
            election_coordinator = ElectionCoordinator(
                my_id=router_index,
                my_host="0.0.0.0",
                my_port=election_port,
                all_nodes=all_nodes,
                on_leader_change=handle_leader_change,
                election_timeout=5.0
            )

            heartbeat_client = HeartbeatClient(
                coordinator=election_coordinator,
                my_id=router_index,
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
                my_id=router_index,
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
        
        router.stop()
        logger.info("Shutdown complete.")

    except (ValueError, MessageMiddlewareDisconnectedError) as e:
        logger.critical(f"Service could not start due to a configuration or connection error: {e}")
    except Exception as e:
        logger.critical(f"An unexpected error occurred during service setup: {e}", exc_info=True)

if __name__ == '__main__':
    main()
