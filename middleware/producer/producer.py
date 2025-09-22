from middleware_client import MessageMiddlewareQueue, MessageMiddlewareDisconnectedError
import time
import os

RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'rabbitmq')
QUEUE_NAME = 'task_queue'

def main():
    middleware = None
    while not middleware:
        try:
            middleware = MessageMiddlewareQueue(host=RABBITMQ_HOST, queue_name=QUEUE_NAME)
            print("Producer connected to RabbitMQ!")
        except MessageMiddlewareDisconnectedError:
            print("Connection to RabbitMQ failed. Retrying in 1 second...")
            time.sleep(1)

    try:
        i = 0
        while True:
            message = f"Hello from the client, message number {i}"
            
            middleware.send(message.encode('utf-8'))
            
            print(f"Sent '{message}'")
            time.sleep(1)
            i += 1
    except KeyboardInterrupt:
        print("Stopping producer.")
    finally:
        if middleware:
            middleware.close()
            print("Producer connection closed.")

if __name__ == '__main__':
    main()