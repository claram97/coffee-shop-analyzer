import unittest
from unittest import mock
import pika

from middleware.middleware_client import (
    MessageMiddlewareQueue,
    MessageMiddlewareExchange,
    MessageMiddlewareDisconnectedError,
)

RABBITMQ_HOST = "localhost"

class TestMessageMiddlewareQueue(unittest.TestCase):
    def setUp(self):
        self.blocking_connection_patcher = mock.patch(
            "middleware.middleware_client.pika.BlockingConnection"
        )
        self.mock_blocking_connection = self.blocking_connection_patcher.start()
        self.addCleanup(self.blocking_connection_patcher.stop)

        self.connection = mock.Mock()
        self.connection.is_open = True
        self.connection.add_callback_threadsafe = mock.Mock()
        self.channel = mock.Mock()
        self.channel.is_open = True
        self.connection.channel.return_value = self.channel
        self.mock_blocking_connection.return_value = self.connection

    def _create_queue(self, queue_name="test_queue"):
        return MessageMiddlewareQueue(RABBITMQ_HOST, queue_name)

    def test_initialization_declares_queue(self):
        queue = self._create_queue()
        self.connection.channel.assert_called_once()
        self.channel.queue_declare.assert_called_once_with(
            queue="test_queue", durable=True
        )
        self.assertIsNotNone(queue._channel)

    def test_send_requires_bytes(self):
        queue = self._create_queue()
        with self.assertRaises(ValueError):
            queue.send("not-bytes")

    def test_send_publishes_message(self):
        queue = self._create_queue()
        queue.send(b"payload")
        self.channel.basic_publish.assert_called_once()
        _, kwargs = self.channel.basic_publish.call_args
        self.assertEqual(kwargs["exchange"], "")
        self.assertEqual(kwargs["routing_key"], "test_queue")
        self.assertEqual(kwargs["body"], b"payload")
        self.assertEqual(kwargs["properties"].delivery_mode, 2)

    def test_callback_wrapper_acknowledges_on_success(self):
        queue = self._create_queue()
        handler = mock.Mock()
        wrapper = queue._create_callback_wrapper(handler)
        method = mock.Mock(delivery_tag="tag")
        channel = mock.Mock()
        queue._stop_event.clear()

        wrapper(channel, method, None, b"body")

        handler.assert_called_once_with(b"body")
        channel.basic_ack.assert_called_once_with(delivery_tag="tag")
        channel.basic_nack.assert_not_called()

    def test_callback_wrapper_nacks_on_failure(self):
        queue = self._create_queue()
        handler = mock.Mock(side_effect=RuntimeError("boom"))
        wrapper = queue._create_callback_wrapper(handler)
        method = mock.Mock(delivery_tag="tag")
        channel = mock.Mock()
        queue._stop_event.clear()

        wrapper(channel, method, None, b"body")

        handler.assert_called_once_with(b"body")
        channel.basic_ack.assert_not_called()
        channel.basic_nack.assert_called_once_with(delivery_tag="tag", requeue=True)

    @mock.patch("middleware.middleware_client.threading.Thread")
    def test_start_consuming_spawns_thread_once(self, thread_cls):
        thread = mock.Mock()
        thread.is_alive.return_value = False
        thread_cls.return_value = thread
        self.channel.basic_consume.return_value = "consumer-tag"
        queue = self._create_queue()

        queue._process_events_until_stopped = mock.Mock()
        queue._cleanup_consumer = mock.Mock()

        queue.start_consuming(mock.Mock())

        queue._stop_event.set()

        thread_cls.assert_called_once()
        thread.start.assert_called_once()

        target = thread_cls.call_args.kwargs["target"]
        args = thread_cls.call_args.kwargs["args"]
        target(*args)

        self.channel.basic_consume.assert_called_once()
        call_kwargs = self.channel.basic_consume.call_args.kwargs
        self.assertEqual(call_kwargs["queue"], "test_queue")
        self.assertIn("on_message_callback", call_kwargs)

        thread.is_alive.return_value = True
        queue.start_consuming(mock.Mock())

        thread_cls.assert_called_once()

    def test_stop_consuming_requests_stop_and_joins(self):
        queue = self._create_queue()

        thread = mock.Mock()
        thread.is_alive.return_value = True
        queue._consuming_thread = thread

        queue.stop_consuming()

        self.connection.add_callback_threadsafe.assert_called_once()
        thread.join.assert_called_once()
        self.assertIsNone(queue._consuming_thread)

    def test_stop_consuming_no_thread(self):
        queue = self._create_queue()
        queue.stop_consuming()
        self.connection.add_callback_threadsafe.assert_not_called()

    def test_delete_removes_queue_when_channel_open(self):
        queue = self._create_queue()

        queue.delete()

        self.channel.queue_delete.assert_called_once_with(queue="test_queue")

    def test_close_closes_connection(self):
        queue = self._create_queue()

        queue.close()

        self.connection.close.assert_called_once()
        self.assertIsNone(queue._connection)
        self.assertIsNone(queue._channel)

    def test_connection_error_raises_custom_exception(self):
        self.mock_blocking_connection.side_effect = pika.exceptions.AMQPConnectionError(
            "failed"
        )

        with self.assertRaises(MessageMiddlewareDisconnectedError):
            MessageMiddlewareQueue(RABBITMQ_HOST, "queue")


class TestMessageMiddlewareExchange(unittest.TestCase):
    def setUp(self):
        self.blocking_connection_patcher = mock.patch(
            "middleware.middleware_client.pika.BlockingConnection"
        )
        self.mock_blocking_connection = self.blocking_connection_patcher.start()
        self.addCleanup(self.blocking_connection_patcher.stop)

        self.connection = mock.Mock()
        self.connection.is_open = True
        self.connection.add_callback_threadsafe = mock.Mock()
        self.channel = mock.Mock()
        self.channel.is_open = True
        self.connection.channel.return_value = self.channel
        self.mock_blocking_connection.return_value = self.connection

    def _create_exchange(self, route_keys=None, consumer=None, queue_name=None):
        keys = route_keys if route_keys is not None else ["route.key"]
        return MessageMiddlewareExchange(
            RABBITMQ_HOST,
            "exchange",
            route_keys=keys,
            consumer=consumer,
            queue_name=queue_name,
        )

    def test_initialization_declares_exchange(self):
        self._create_exchange()

        self.channel.exchange_declare.assert_called_once_with(
            exchange="exchange", exchange_type="topic", durable=True
        )

    def test_initialization_with_consumer_binds_queue(self):
        exchange = self._create_exchange(
            route_keys=["key.one", "key.two"], consumer="consumer", queue_name="queue"
        )

        self.channel.queue_declare.assert_called_once_with(queue="queue", durable=True)
        self.assertEqual(self.channel.queue_bind.call_count, 2)
        self.channel.queue_bind.assert_any_call(
            exchange="exchange", queue="queue", routing_key="key.one"
        )
        self.channel.queue_bind.assert_any_call(
            exchange="exchange", queue="queue", routing_key="key.two"
        )
        self.channel.basic_qos.assert_called_once_with(prefetch_count=3)
        self.assertEqual(exchange.default_routing_key, "key.one")

    def test_send_requires_bytes(self):
        exchange = self._create_exchange()

        with self.assertRaises(ValueError):
            exchange.send("not-bytes")

    def test_send_publishes_message(self):
        exchange = self._create_exchange(route_keys=["topic.key"])

        exchange.send(b"payload")

        self.channel.basic_publish.assert_called_once()
        _, kwargs = self.channel.basic_publish.call_args
        self.assertEqual(kwargs["exchange"], "exchange")
        self.assertEqual(kwargs["routing_key"], "topic.key")
        self.assertEqual(kwargs["body"], b"payload")
        self.assertEqual(kwargs["properties"].delivery_mode, 2)

    def test_callback_wrapper_acknowledges_on_success(self):
        exchange = self._create_exchange()

        handler = mock.Mock()
        wrapper = exchange._create_callback_wrapper(handler)
        method = mock.Mock(delivery_tag="tag")
        channel = mock.Mock()
        exchange._stop_event.clear()

        wrapper(channel, method, None, b"body")

        handler.assert_called_once_with(b"body")
        channel.basic_ack.assert_called_once_with(delivery_tag="tag")
        channel.basic_nack.assert_not_called()

    def test_callback_wrapper_nacks_on_failure(self):
        exchange = self._create_exchange()

        handler = mock.Mock(side_effect=RuntimeError("boom"))
        wrapper = exchange._create_callback_wrapper(handler)
        method = mock.Mock(delivery_tag="tag")
        channel = mock.Mock()
        exchange._stop_event.clear()

        wrapper(channel, method, None, b"body")

        handler.assert_called_once_with(b"body")
        channel.basic_ack.assert_not_called()
        channel.basic_nack.assert_called_once_with(delivery_tag="tag", requeue=True)

    @mock.patch("middleware.middleware_client.threading.Thread")
    def test_start_consuming_declares_queue_and_starts_thread(self, thread_cls):
        thread = mock.Mock()
        thread.is_alive.return_value = False
        thread_cls.return_value = thread
        result = mock.Mock()
        result.method.queue = "generated"
        self.channel.queue_declare.return_value = result
        self.channel.basic_consume.return_value = "consumer-tag"
        exchange = self._create_exchange(route_keys=["topic.*"])

        exchange._process_events_until_stopped = mock.Mock()
        exchange._cleanup_consumer = mock.Mock()

        exchange.start_consuming(mock.Mock())

        exchange._stop_event.set()

        self.channel.queue_bind.assert_called_once_with(
            exchange="exchange", queue="generated", routing_key="topic.*"
        )
        thread_cls.assert_called_once()
        thread.start.assert_called_once()

        target = thread_cls.call_args.kwargs["target"]
        args = thread_cls.call_args.kwargs["args"]
        target(*args)

        self.channel.basic_consume.assert_called_once()
        call_kwargs = self.channel.basic_consume.call_args.kwargs
        self.assertEqual(call_kwargs["queue"], "generated")
        self.assertIn("on_message_callback", call_kwargs)

    def test_stop_consuming_requests_stop_and_joins(self):
        exchange = self._create_exchange()

        thread = mock.Mock()
        thread.is_alive.return_value = True
        exchange._consuming_thread = thread

        exchange.stop_consuming()

        self.connection.add_callback_threadsafe.assert_called_once()
        thread.join.assert_called_once()
        self.assertIsNone(exchange._consuming_thread)

    def test_stop_consuming_no_thread(self):
        exchange = self._create_exchange()
        exchange.stop_consuming()
        self.connection.add_callback_threadsafe.assert_not_called()

    def test_delete_exchange_when_channel_open(self):
        exchange = self._create_exchange()

        exchange.delete()

        self.channel.exchange_delete.assert_called_once_with(exchange="exchange")

    def test_close_closes_connection(self):
        exchange = self._create_exchange()

        exchange.close()

        self.connection.close.assert_called_once()
        self.assertIsNone(exchange._connection)
        self.assertIsNone(exchange._channel)

    def test_requires_at_least_one_route_key(self):
        with self.assertRaises(ValueError):
            MessageMiddlewareExchange(RABBITMQ_HOST, "exchange", route_keys=[])


if __name__ == "__main__":
    unittest.main()
