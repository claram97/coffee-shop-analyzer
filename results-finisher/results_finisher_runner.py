import os
import signal
import threading
import time
import logging
from results_finisher import ResultsFinisher
from middleware_client import MessageMiddlewareQueue, MessageMiddlewareDisconnectedError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [Main] - %(message)s')

def main():
    rabbitmq_host = os.getenv("RABBITMQ_HOST", "localhost")
    input_queue_name = os.getenv("INPUT_QUEUE", "results_finisher_queue_1")
    output_queue_name = os.getenv("OUTPUT_QUEUE", "orchestrator_results_queue")
    checkpoint_dir = os.getenv("CHECKPOINT_DIR", "/checkpoints/finisher")

    print("--- ResultsFinisher Service Configuration ---")
    print(f"RabbitMQ Host: {rabbitmq_host}")
    print(f"Input Queue: {input_queue_name}")
    print(f"Output Queue: {output_queue_name}")
    print(f"Checkpoint Directory: {checkpoint_dir}")
    print("-------------------------------------------")

    try:
        input_client = MessageMiddlewareQueue(host=rabbitmq_host, queue_name=input_queue_name)
        output_client = MessageMiddlewareQueue(host=rabbitmq_host, queue_name=output_queue_name)

        finisher = ResultsFinisher(
            input_client=input_client,
            output_client=output_client,
            checkpoint_dir=checkpoint_dir
        )
        
        shutdown_event = threading.Event()
        def shutdown_handler(signum, frame):
            print("\nShutdown signal received. Stopping finisher...")
            finisher.stop()
            shutdown_event.set()
            
        signal.signal(signal.SIGINT, shutdown_handler)
        signal.signal(signal.SIGTERM, shutdown_handler)

        finisher.start()
        
        print("Service is running. Press Ctrl+C to exit.")
        shutdown_event.wait()
        
        print("Shutdown complete.")

    except MessageMiddlewareDisconnectedError as e:
        print(f"Could not connect to RabbitMQ. Aborting. Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred during setup: {e}", exc_info=True)

if __name__ == '__main__':
    main()