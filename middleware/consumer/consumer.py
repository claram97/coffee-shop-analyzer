from middleware_client import MessageMiddlewareQueue, MessageMiddlewareDisconnectedError
import time
import os

RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'rabbitmq')
QUEUE_NAME = 'task_queue'

# Aca se define como se maneja cada mensaje recibido. Recordar que se recibe en bytes.
def message_handler(body):
    message = body.decode('utf-8')
    print(f"Received and processed: '{message}'")
    time.sleep(1)

def main():
    middleware = None
    while not middleware:
        try:
            middleware = MessageMiddlewareQueue(host=RABBITMQ_HOST, queue_name=QUEUE_NAME)
            print("Consumer connected to RabbitMQ!")
        except MessageMiddlewareDisconnectedError:
            print("Connection to RabbitMQ failed. Retrying in 1 second...")
            time.sleep(1)

    try:
        # La idea es que el consumidor corra en un hilo aparte
        middleware.start_consuming(on_message_callback=message_handler)
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\nStopping consumer.")
    finally:
        if middleware:
            middleware.close()
            print("Consumer connection closed.")

if __name__ == '__main__':
    main()