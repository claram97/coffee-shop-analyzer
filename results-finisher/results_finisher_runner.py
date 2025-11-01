import os
import signal
import threading
import logging
from results_finisher import ResultsFinisher
from middleware.middleware_client import MessageMiddlewareQueue, MessageMiddlewareDisconnectedError
from leader_election import ElectionCoordinator

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [Main] - %(message)s')

def main():
    rabbitmq_host = os.getenv("RABBITMQ_HOST", "localhost")
    input_queue_name = os.getenv("INPUT_QUEUE", "results_finisher_queue_1")
    output_queue_name = os.getenv("OUTPUT_QUEUE", "orchestrator_results_queue")
    
    # Results finishers don't have explicit index but we can derive from hostname
    finisher_index = hash(os.getenv("HOSTNAME", "results-finisher")) % 100
    election_port = int(os.getenv("ELECTION_PORT", 9600 + finisher_index))
        
    logging.info("--- ResultsFinisher Service (In-Memory) ---")
    logging.info(f"RabbitMQ Host: {rabbitmq_host}")
    logging.info(f"Input Queue: {input_queue_name}")
    logging.info(f"Output Queue: {output_queue_name}")
    logging.info("-------------------------------------------")

    try:
        input_client = MessageMiddlewareQueue(host=rabbitmq_host, queue_name=input_queue_name)
        output_client = MessageMiddlewareQueue(host=rabbitmq_host, queue_name=output_queue_name)

        # Initialize election coordinator (listener only for now)
        election_coordinator = None
        try:
            from app_config.config_loader import Config
            cfg_path = os.getenv("CONFIG_PATH", "/config/config.ini")
            cfg = Config(cfg_path)
            total_results_workers = cfg.workers.results
            
            # Build list of all results finisher nodes in the cluster
            all_nodes = [
                (i, f"results-finisher-{i}", 9600 + i)
                for i in range(total_results_workers)
            ]
            
            logging.info(f"Initializing election coordinator for results-finisher-{finisher_index} on port {election_port}")
            logging.info(f"Cluster nodes: {all_nodes}")
            
            election_coordinator = ElectionCoordinator(
                my_id=finisher_index,
                my_host="0.0.0.0",
                my_port=election_port,
                all_nodes=all_nodes,
                on_leader_change=None,
                election_timeout=5.0
            )
            
            election_coordinator.start()
            logging.info(f"Election listener started on port {election_port}")
            
        except Exception as e:
            logging.error(f"Failed to initialize election coordinator: {e}", exc_info=True)
            election_coordinator = None

        finisher = ResultsFinisher(
            input_client=input_client,
            output_client=output_client
        )
        
        shutdown_event = threading.Event()
        def shutdown_handler(signum, frame):
            logging.info("Shutdown signal received. Stopping finisher...")
            shutdown_event.set()
            
        signal.signal(signal.SIGINT, shutdown_handler)
        signal.signal(signal.SIGTERM, shutdown_handler)

        finisher.start()
        
        logging.info("Service is running. Press Ctrl+C to exit.")
        shutdown_event.wait()
        
        logging.info("Cleaning up resources...")
        
        if election_coordinator:
            logging.info("Stopping election coordinator...")
            election_coordinator.stop()
        
        finisher.stop()
        logging.info("Shutdown complete.")

    except MessageMiddlewareDisconnectedError as e:
        logging.critical(f"Could not connect to RabbitMQ. Aborting. Error: {e}")
    except Exception as e:
        # FIX: Use logger to correctly show exception info
        logging.critical(f"An unexpected error occurred during setup: {e}", exc_info=True)

if __name__ == '__main__':
    main()