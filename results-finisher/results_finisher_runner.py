import os
import signal
import threading
import logging
from results_finisher import ResultsFinisher
from middleware_client import MessageMiddlewareQueue, MessageMiddlewareDisconnectedError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [Main] - %(message)s')

def main():
    rabbitmq_host = os.getenv("RABBITMQ_HOST", "localhost")
    input_queue_name = os.getenv("INPUT_QUEUE", "results_finisher_queue_1")
    output_queue_name = os.getenv("OUTPUT_QUEUE", "orchestrator_results_queue")
        
    logging.info("--- ResultsFinisher Service (In-Memory) ---")
    logging.info(f"RabbitMQ Host: {rabbitmq_host}")
    logging.info(f"Input Queue: {input_queue_name}")
    logging.info(f"Output Queue: {output_queue_name}")
    logging.info("-------------------------------------------")

    try:
        input_client = MessageMiddlewareQueue(host=rabbitmq_host, queue_name=input_queue_name)
        output_client = MessageMiddlewareQueue(host=rabbitmq_host, queue_name=output_queue_name)

        finisher = ResultsFinisher(
            input_client=input_client,
            output_client=output_client
        )
        
        shutdown_event = threading.Event()
        def shutdown_handler(signum, frame):
            logging.info("Shutdown signal received. Stopping finisher...")
            finisher.stop()
            shutdown_event.set()
            
        signal.signal(signal.SIGINT, shutdown_handler)
        signal.signal(signal.SIGTERM, shutdown_handler)

        finisher.start()
        
        logging.info("Service is running. Press Ctrl+C to exit.")
        shutdown_event.wait()
        
        logging.info("Shutdown complete.")

    except MessageMiddlewareDisconnectedError as e:
        logging.critical(f"Could not connect to RabbitMQ. Aborting. Error: {e}")
    except Exception as e:
        # FIX: Use logger to correctly show exception info
        logging.critical(f"An unexpected error occurred during setup: {e}", exc_info=True)

if __name__ == '__main__':
    main()