import unittest
import pika
import os
import sys

def check_rabbitmq():
    """Check if RabbitMQ is reachable."""
    rabbitmq_host = os.getenv("RABBITMQ_HOST", "localhost")
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host))
        connection.close()
        print(f"RabbitMQ is reachable at {rabbitmq_host}.")
    except Exception as e:
        print(f"Error: Unable to connect to RabbitMQ at {rabbitmq_host}. Ensure it is running.")
        print(f"Details: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # Check RabbitMQ availability
    check_rabbitmq()

    # Discover and run all tests in the current directory
    loader = unittest.TestLoader()
    suite = loader.discover(start_dir=".", pattern="*_test.py")

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    # Exit with the appropriate code
    exit(0 if result.wasSuccessful() else 1)