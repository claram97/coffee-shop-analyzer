import pika
import threading
from abc import ABC, abstractmethod
import time

class MessageMiddlewareMessageError(Exception):
    pass

class MessageMiddlewareDisconnectedError(Exception):
    pass

class MessageMiddlewareCloseError(Exception):
    pass

class MessageMiddlewareDeleteError(Exception):
    pass

class MessageMiddleware(ABC):
    @abstractmethod
    def start_consuming(self, on_message_callback):
        pass

    @abstractmethod
    def stop_consuming(self):
        pass

    @abstractmethod
    def send(self, message):
        pass

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def delete(self):
        pass

class RabbitMQBase(MessageMiddleware):
    def __init__(self, host):
        self._host = host
        self._connection = None
        self._channel = None
        self._consumer_tag = None
        self._consuming_thread = None
        self._stop_event = threading.Event()
        self._consume_lock = threading.Lock()
        self.queue_name = None
        self._connect()

    # Still need to handle reconnections!
    def _connect(self):
        try:
            self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=self._host))
            self._channel = self._connection.channel()
        except (pika.exceptions.AMQPConnectionError, OSError, Exception) as e:
            raise MessageMiddlewareDisconnectedError(f"Could not connect to RabbitMQ on '{self._host}'") from e

    def start_consuming(self, on_message_callback):
        if not self.queue_name:
            raise MessageMiddlewareMessageError("Queue name not set")
            
        with self._consume_lock:
            if self._consuming_thread and self._consuming_thread.is_alive():
                return
            
            self._stop_event.clear()
            callback_wrapper = self._create_callback_wrapper(on_message_callback)
            
            try:
                self._consuming_thread = threading.Thread(
                    target=self._consume_loop, 
                    args=(callback_wrapper,), 
                    daemon=True
                )
                self._consuming_thread.start()
            except Exception as e:
                raise MessageMiddlewareMessageError("Error starting consumer") from e

    def _create_callback_wrapper(self, on_message_callback):
        def callback_wrapper(ch, method, properties, body):
            if self._stop_event.is_set():
                return
            try:
                on_message_callback(body)
                ch.basic_ack(delivery_tag=method.delivery_tag)
            except Exception as e:
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        return callback_wrapper

    def _consume_loop(self, callback_wrapper):
        try:
            self._consumer_tag = self._channel.basic_consume(
                queue=self.queue_name, 
                on_message_callback=callback_wrapper
            )
            
            self._process_events_until_stopped()
            self._cleanup_consumer()
            
        except pika.exceptions.AMQPError as e:
            if not self._stop_event.is_set():
                raise MessageMiddlewareMessageError("Consumer error") from e

    def _process_events_until_stopped(self):
        """Process RabbitMQ events until stop is requested"""
        while not self._stop_event.is_set():
            try:
                self._connection.process_data_events(time_limit=1.0)
            except Exception as e:
                if not self._stop_event.is_set():
                    break

    def _cleanup_consumer(self):
        """Clean up consumer resources"""
        if self._consumer_tag and self._channel.is_open:
            try:
                self._channel.basic_cancel(self._consumer_tag)
            except Exception:
                pass
            self._consumer_tag = None

    def stop_consuming(self):
        with self._consume_lock:
            if not self._consuming_thread or not self._consuming_thread.is_alive():
                return
            
            try:
                self._stop_event.set()
                
                self._consuming_thread.join(timeout=1.0)
                
                if self._consuming_thread.is_alive():
                    self._force_cleanup()
                
            except Exception as e:
                raise MessageMiddlewareDisconnectedError("Error stopping consumer") from e
            finally:
                self._consuming_thread = None
                self._consumer_tag = None

    def _force_cleanup(self):
        try:
            if self._connection and self._connection.is_open:
                self._connection.close()
            self._connection = None
            self._channel = None
        except Exception:
            pass

    def close(self):
        try:
            self.stop_consuming()
            if self._connection and self._connection.is_open:
                self._connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError("Error closing connection") from e

class MessageMiddlewareQueue(RabbitMQBase):
    def __init__(self, host, queue_name):
        super().__init__(host)
        self.queue_name = queue_name  # Set after parent init
        self._setup_queue()

    def _setup_queue(self):
        # durable=True makes sure the queue survives broker restarts.
        self._channel.queue_declare(queue=self.queue_name, durable=True)
        # Amount of unacknowledged messages a consumer can have.
        self._channel.basic_qos(prefetch_count=3)

    def send(self, message):
        try:
            if not isinstance(message, bytes):
                raise ValueError("Message must be bytes")
                
            self._channel.basic_publish(
                exchange='',
                routing_key=self.queue_name,
                body=message,
                properties=pika.BasicProperties(delivery_mode=2)
            )
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError("Error sending message") from e

    def delete(self):
        try:
            self.stop_consuming()
            if self._channel and self._channel.is_open:
                self._channel.queue_delete(queue=self.queue_name)
        except Exception as e:
            raise MessageMiddlewareDeleteError(f"Error al eliminar la cola '{self.queue_name}'") from e


class MessageMiddlewareExchange(RabbitMQBase):
    def __init__(self, host, exchange_name, route_keys):
        self.exchange_name = exchange_name
        self.route_keys = route_keys if isinstance(route_keys, list) else [route_keys]

        if not self.route_keys:
            raise ValueError("MessageMiddlewareExchange requires at least one route key for sending.")
        self.default_routing_key = self.route_keys[0]
        
        super().__init__(host)
        self._setup_exchange()

    def _setup_exchange(self):
        self._channel.exchange_declare(exchange=self.exchange_name, exchange_type='topic', durable=True)
        result = self._channel.queue_declare(queue='', exclusive=True)
        self.queue_name = result.method.queue
        for key in self.route_keys:
            self._channel.queue_bind(
                exchange=self.exchange_name,
                queue=self.queue_name,
                routing_key=key
            )

    def send(self, message):
        try:
            if not isinstance(message, bytes):
                raise ValueError("Message must be bytes")
                
            self._channel.basic_publish(
                exchange=self.exchange_name,
                routing_key=self.default_routing_key,
                body=message,
                properties=pika.BasicProperties(delivery_mode=2)
            )
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError("Error sending message") from e

    def delete(self):
        try:
            self.stop_consuming()
            if self._channel and self._channel.is_open:
                self._channel.exchange_delete(exchange=self.exchange_name)
        except Exception as e:
            raise MessageMiddlewareDeleteError(f"Error deleting exchange '{self.exchange_name}'") from e