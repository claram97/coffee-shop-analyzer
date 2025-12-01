import logging
import threading
import time
from abc import ABC, abstractmethod

import pika


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


class MessageMiddlewareQueue(MessageMiddleware):
    def __init__(self, host, queue_name):
        self._host = host
        self.queue_name = queue_name
        self._connection = None
        self._channel = None
        self._consumer_tag = None
        self._consuming_thread = None
        self._stop_event = threading.Event()
        self._consume_lock = threading.Lock()
        self._current_callback = None  # Stored for reconnection
        self._connect()
        self._setup_queue()

    def _connect(self):
        try:
            self._connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=self._host, heartbeat=1200, blocked_connection_timeout=600
                )
            )
            self._channel = self._connection.channel()
        except (pika.exceptions.AMQPConnectionError, OSError) as e:
            raise MessageMiddlewareDisconnectedError(
                f"Could not connect to RabbitMQ on '{self._host}'"
            ) from e

    def _setup_queue(self):
        """Declare the queue to ensure it exists."""
        try:
            self._channel.queue_declare(queue=self.queue_name, durable=True)
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(
                f"Error declaring queue '{self.queue_name}'"
            ) from e

    def start_consuming(self, on_message_callback):
        with self._consume_lock:
            if self._consuming_thread and self._consuming_thread.is_alive():
                return

            if not self.queue_name:
                logging.error("Queue name not set")
                return

            self._stop_event.clear()
            callback_wrapper = self._create_callback_wrapper(on_message_callback)

            try:
                self._consuming_thread = threading.Thread(
                    target=self._consume_loop, args=(callback_wrapper,), daemon=True
                )
                self._consuming_thread.start()
            except Exception as e:
                raise MessageMiddlewareMessageError("Error starting consumer") from e

    def _create_callback_wrapper(self, on_message_callback):
        def callback_wrapper(ch, method, properties, body):
            logging.debug(
                f"DEBUG: Callback received message, delivery_tag: {method.delivery_tag}, body_size: {len(body)}, redelivered: {method.redelivered}"
            )

            # During shutdown, NACK the message so it's requeued and processed after restart.
            # This ensures we never lose messages - they'll be redelivered.
            if self._stop_event.is_set():
                logging.warning("DEBUG: Stop event is set, NACKing message for redelivery")
                try:
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                except Exception as e:
                    logging.warning("Failed to NACK during shutdown: %s", e)
                return
            try:
                logging.debug(f"DEBUG: About to call on_message_callback")
                # Try calling with additional context first (new API)
                try:
                    result = on_message_callback(
                        body,
                        channel=ch,
                        delivery_tag=method.delivery_tag,
                        redelivered=method.redelivered,
                    )
                except TypeError:
                    # Fallback to old API (callback only accepts body)
                    result = on_message_callback(body)

                # If callback returns True or None, ack immediately
                # If callback returns False, don't ack (manual ack required)
                should_ack = result if result is not None else True

                if should_ack:
                    logging.debug(
                        f"DEBUG: Message processed successfully, sending ACK for delivery_tag: {method.delivery_tag}"
                    )
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                else:
                    logging.debug(
                        f"DEBUG: Message processed, ACK delayed (manual) for delivery_tag: {method.delivery_tag}"
                    )
            except Exception as e:
                logging.error(
                    f"DEBUG: Exception in message callback: {e}, sending NACK for delivery_tag: {method.delivery_tag}"
                )
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

        return callback_wrapper

    def _consume_loop(self, callback_wrapper):
        try:
            # Store callback for potential reconnection
            self._current_callback = callback_wrapper
            self._consumer_tag = self._channel.basic_consume(
                queue=self.queue_name, on_message_callback=callback_wrapper
            )

            self._process_events_until_stopped()
            self._cleanup_consumer()

        except pika.exceptions.AMQPError as e:
            if not self._stop_event.is_set():
                raise MessageMiddlewareMessageError("Consumer error") from e

    def _process_events_until_stopped(self):
        """Process RabbitMQ events until stop is requested, with automatic reconnection"""
        reconnect_delay = 1.0
        max_reconnect_delay = 30.0

        while not self._stop_event.is_set():
            try:
                self._connection.process_data_events(time_limit=10.0)
                reconnect_delay = 1.0  # Reset delay on successful processing
            except Exception as e:
                if self._stop_event.is_set():
                    break

                logging.error(
                    f"Exception in process_data_events: {e} - attempting reconnect in {reconnect_delay}s",
                    exc_info=True,
                )

                # Wait before reconnecting (with stop event check)
                if self._stop_event.wait(timeout=reconnect_delay):
                    break

                # Attempt to reconnect
                try:
                    self._force_cleanup()
                    self._connect()
                    self._setup_queue()
                    # Re-register consumer with same callback
                    if hasattr(self, '_current_callback') and self._current_callback:
                        self._consumer_tag = self._channel.basic_consume(
                            queue=self.queue_name, on_message_callback=self._current_callback
                        )
                    logging.info(f"Successfully reconnected to queue '{self.queue_name}'")
                    reconnect_delay = 1.0  # Reset delay on successful reconnect
                except Exception as reconnect_error:
                    logging.error(
                        f"Reconnection failed: {reconnect_error} - will retry in {reconnect_delay}s"
                    )
                    reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)

    def _cleanup_consumer(self):
        """Clean up consumer resources"""
        if self._consumer_tag and self._channel and self._channel.is_open:
            try:
                self._channel.basic_cancel(self._consumer_tag)
            except Exception:
                pass
            self._consumer_tag = None

    def stop_consuming(self):
        """
        Thread-safely stops the consumer by scheduling its internal stop event to be set.
        """
        with self._consume_lock:
            if not self._consuming_thread or not self._consuming_thread.is_alive():
                return

            logging.info(f"Scheduling stop for consumer on queue '{self.queue_name}'")

            if self._connection and self._connection.is_open:
                self._connection.add_callback_threadsafe(self._stop_event.set)

        self._consuming_thread.join()
        logging.info(
            f"Consumer thread for queue '{self.queue_name}' has successfully joined."
        )
        self._consuming_thread = None

    def _force_cleanup(self):
        try:
            if self._connection and self._connection.is_open:
                self._connection.close()
            self._connection = None
            self._channel = None
        except Exception:
            pass

    def send(self, message):
        try:
            if not isinstance(message, bytes):
                raise ValueError("Message must be bytes")

            self._channel.basic_publish(
                exchange="",
                routing_key=self.queue_name,
                body=message,
                properties=pika.BasicProperties(delivery_mode=2),
            )
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError("Error sending message") from e

    def close(self):
        try:
            self.stop_consuming()
            if self._connection and self._connection.is_open:
                self._connection.close()
            self._connection = None
            self._channel = None
        except Exception as e:
            raise MessageMiddlewareCloseError("Error closing connection") from e

    def delete(self):
        try:
            self.stop_consuming()
            if self._channel and self._channel.is_open:
                self._channel.queue_delete(queue=self.queue_name)
        except Exception as e:
            raise MessageMiddlewareDeleteError(
                f"Error deleting queue '{self.queue_name}'"
            ) from e


class MessageMiddlewareExchange(MessageMiddleware):
    def __init__(self, host, exchange_name, route_keys, consumer=None, queue_name=None):
        self._host = host
        self.exchange_name = exchange_name
        self.route_keys = route_keys if isinstance(route_keys, list) else [route_keys]

        if not self.route_keys:
            raise ValueError(
                "MessageMiddlewareExchange requires at least one route key for sending."
            )
        self.default_routing_key = self.route_keys[0]

        self._connection = None
        self._channel = None
        self._consumer_tag = consumer
        self._consuming_thread = None
        self._stop_event = threading.Event()
        self._consume_lock = threading.Lock()
        self.queue_name = queue_name
        self._send_lock = threading.Lock()
        self._current_callback = None  # Stored for reconnection

        self._connect()
        self._setup_exchange()
        if queue_name:
            try:
                self._channel.queue_declare(queue=self.queue_name, durable=True)
                for key in self.route_keys:
                    self._channel.queue_bind(
                        exchange=self.exchange_name,
                        queue=self.queue_name,
                        routing_key=key,
                    )
                if consumer:
                    self._channel.basic_qos(prefetch_count=100)
                logging.debug(
                    f"[MMX] ensured binding ex={self.exchange_name} q={self.queue_name} rk={self.route_keys}"
                )
            except Exception as e:
                logging.error(f"[MMX] ensure binding failed: {e}", exc_info=True)
                raise

    def _connect(self):
        try:
            self._connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=self._host, heartbeat=1200, blocked_connection_timeout=600
                )
            )
            self._channel = self._connection.channel()
        except (pika.exceptions.AMQPConnectionError, OSError, Exception) as e:
            raise MessageMiddlewareDisconnectedError(
                f"Could not connect to RabbitMQ on '{self._host}'"
            ) from e

    def _setup_exchange(self):
        """Declare the exchange but don't create a queue yet"""
        self._channel.exchange_declare(
            exchange=self.exchange_name, exchange_type="topic", durable=True
        )

    def start_consuming(self, on_message_callback):
        with self._consume_lock:
            if self._consuming_thread and self._consuming_thread.is_alive():
                return

            # Declare queue - either named (durable) or anonymous (exclusive)
            if not self.queue_name:
                result = self._channel.queue_declare(queue="", exclusive=True)
                self.queue_name = result.method.queue
            else:
                # Declare named queue as durable
                self._channel.queue_declare(queue=self.queue_name, durable=True)

            for key in self.route_keys:
                try:
                    self._channel.queue_bind(
                        exchange=self.exchange_name,
                        queue=self.queue_name,
                        routing_key=key,
                    )
                except Exception as e:
                    logging.error(
                        "Failed to bind queue %s to exchange %s (rk=%s): %s",
                        self.queue_name,
                        self.exchange_name,
                        key,
                        e,
                    )
                    raise

            self._channel.basic_qos(prefetch_count=100)

            self._stop_event.clear()
            callback_wrapper = self._create_callback_wrapper(on_message_callback)
            self._consuming_thread = threading.Thread(
                target=self._consume_loop, args=(callback_wrapper,), daemon=True
            )
            self._consuming_thread.start()

    def _create_callback_wrapper(self, on_message_callback):
        def callback_wrapper(ch, method, properties, body):
            logging.debug(
                f"Exchange callback received message, delivery_tag: {method.delivery_tag}, body_size: {len(body)}, redelivered: {method.redelivered}"
            )

            # During shutdown, NACK the message so it's requeued and processed after restart.
            # This ensures we never lose messages - they'll be redelivered.
            if self._stop_event.is_set():
                logging.warning("Exchange stop event is set, NACKing message for redelivery")
                try:
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                except Exception as e:
                    logging.warning("Failed to NACK during shutdown: %s", e)
                return

            try:
                logging.debug("About to call exchange message callback")
                # Try calling with additional context first (new API)
                try:
                    result = on_message_callback(
                        body,
                        channel=ch,
                        delivery_tag=method.delivery_tag,
                        redelivered=method.redelivered,
                    )
                except TypeError:
                    # Fallback to old API (callback only accepts body)
                    result = on_message_callback(body)

                # If callback returns True or None, ack immediately
                # If callback returns False, don't ack (manual ack required)
                should_ack = result if result is not None else True

                if should_ack:
                    logging.debug(
                        f"Exchange message processed, sending ACK for delivery_tag: {method.delivery_tag}"
                    )
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                else:
                    logging.debug(
                        f"Exchange message processed, ACK delayed (manual) for delivery_tag: {method.delivery_tag}"
                    )
            except Exception as e:
                logging.error(
                    f"Exception in exchange message callback: {e}, sending NACK",
                    exc_info=True,
                )
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

        return callback_wrapper

    def _consume_loop(self, callback_wrapper):
        try:
            logging.debug(f"Starting consume loop on queue: {self.queue_name}")
            # Store callback for potential reconnection
            self._current_callback = callback_wrapper
            self._consumer_tag = self._channel.basic_consume(
                queue=self.queue_name, on_message_callback=callback_wrapper
            )
            logging.debug("Entering event processing loop")

            reconnect_delay = 1.0
            max_reconnect_delay = 30.0

            while not self._stop_event.is_set():
                try:
                    self._connection.process_data_events(time_limit=10.0)
                    reconnect_delay = 1.0  # Reset delay on successful processing
                except Exception as e:
                    if self._stop_event.is_set():
                        break

                    logging.error(
                        f"Exception in exchange process_data_events: {e} - attempting reconnect in {reconnect_delay}s",
                        exc_info=True,
                    )

                    # Wait before reconnecting (with stop event check)
                    if self._stop_event.wait(timeout=reconnect_delay):
                        break

                    # Attempt to reconnect
                    try:
                        self._force_cleanup()
                        self._connect()
                        self._setup_exchange()
                        # Recreate queue and bindings
                        if self.queue_name:
                            self._channel.queue_declare(queue=self.queue_name, durable=True)
                            for key in self.route_keys:
                                self._channel.queue_bind(
                                    exchange=self.exchange_name,
                                    queue=self.queue_name,
                                    routing_key=key,
                                )
                            self._channel.basic_qos(prefetch_count=100)
                        # Re-register consumer
                        self._consumer_tag = self._channel.basic_consume(
                            queue=self.queue_name, on_message_callback=callback_wrapper
                        )
                        logging.info(f"Successfully reconnected to exchange '{self.exchange_name}' queue '{self.queue_name}'")
                        reconnect_delay = 1.0  # Reset delay on successful reconnect
                    except Exception as reconnect_error:
                        logging.error(
                            f"Exchange reconnection failed: {reconnect_error} - will retry in {reconnect_delay}s"
                        )
                        reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)

            if self._consumer_tag and self._channel and self._channel.is_open:
                try:
                    self._channel.basic_cancel(self._consumer_tag)
                except Exception as e:
                    logging.error(f"Error canceling consumer: {e}")
                self._consumer_tag = None

        except pika.exceptions.AMQPError as e:
            if not self._stop_event.is_set():
                logging.error(f"AMQP error in consume loop: {e}")
                raise MessageMiddlewareMessageError("Consumer error") from e

    def _cleanup_consumer(self):
        """Helper to be called on the consumer thread to stop consumption."""
        if self._channel and self._channel.is_open:
            try:
                if self._consumer_tag:
                    self._channel.basic_cancel(self._consumer_tag)
            except Exception as e:
                logging.error(f"Error during basic_cancel for exchange consumer: {e}")
        if self._connection and self._connection.is_open:
            self._connection.close()

    def stop_consuming(self):
        """
        Thread-safely stops the consumer by scheduling its internal stop event to be set.
        """
        with self._consume_lock:
            if not self._consuming_thread or not self._consuming_thread.is_alive():
                return

            logging.info(
                f"Scheduling stop for consumer on exchange '{self.exchange_name}'"
            )

            if self._connection and self._connection.is_open:
                self._connection.add_callback_threadsafe(self._stop_event.set)

        self._consuming_thread.join()
        logging.info(
            f"Consumer thread for exchange '{self.exchange_name}' has successfully joined."
        )
        self._consuming_thread = None

    def _force_cleanup(self):
        try:
            if self._connection and self._connection.is_open:
                self._connection.close()
            self._connection = None
            self._channel = None
        except Exception as e:
            logging.error(f"Error during force cleanup: {e}")

    def send(self, message):
        try:
            if not isinstance(message, bytes):
                raise ValueError("Message must be bytes")

            logging.debug(
                f"Publishing message to exchange {self.exchange_name} with routing key {self.default_routing_key}"
            )
            with self._send_lock:
                self._channel.basic_publish(
                    exchange=self.exchange_name,
                    routing_key=self.default_routing_key,
                    body=message,
                    properties=pika.BasicProperties(delivery_mode=2),
                )
        except pika.exceptions.AMQPError as e:
            logging.error(f"Error sending message to exchange: {e}")
            raise MessageMiddlewareMessageError("Error sending message") from e

    def close(self):
        try:
            self.stop_consuming()
            if self._connection and self._connection.is_open:
                self._connection.close()
            self._connection = None
            self._channel = None
        except Exception as e:
            logging.error(f"Error closing connection: {e}")
            raise MessageMiddlewareCloseError("Error closing connection") from e

    def delete(self):
        try:
            self.stop_consuming()
            if self._channel and self._channel.is_open:
                self._channel.exchange_delete(exchange=self.exchange_name)
        except Exception as e:
            logging.error(f"Error deleting exchange: {e}")
            raise MessageMiddlewareDeleteError(
                f"Error deleting exchange '{self.exchange_name}'"
            ) from e
