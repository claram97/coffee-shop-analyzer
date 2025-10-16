import os
import threading
import time
import unittest
import uuid

import pika

from middleware_client import (
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareExchange,
    MessageMiddlewareQueue,
)

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
MESSAGE_TIMEOUT = float(os.getenv("MIDDLEWARE_TEST_TIMEOUT", "5"))


def _unique_name(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:10]}"


class TestRabbitMQClient(unittest.TestCase):
    def test_work_queue_1_to_1(self):
        queue_name = _unique_name("work_queue_single")
        producer = MessageMiddlewareQueue(RABBITMQ_HOST, queue_name)
        consumer = MessageMiddlewareQueue(RABBITMQ_HOST, queue_name)

        message_received = []
        event = threading.Event()

        def callback(msg):
            message_received.append(msg)
            event.set()

        consumer.start_consuming(callback)
        time.sleep(0.3)

        sent_message = "data_package_1"
        producer.send(sent_message.encode("utf-8"))

        self.assertTrue(event.wait(MESSAGE_TIMEOUT))
        self.assertEqual(sent_message.encode("utf-8"), message_received[0])

        producer.close()
        consumer.delete()
        consumer.close()

    def test_work_queue_1_to_N(self):
        queue_name = _unique_name("work_queue_multi")
        producer = MessageMiddlewareQueue(RABBITMQ_HOST, queue_name)
        consumer1 = MessageMiddlewareQueue(RABBITMQ_HOST, queue_name)
        consumer2 = MessageMiddlewareQueue(RABBITMQ_HOST, queue_name)

        received_messages = []
        lock = threading.Lock()
        event = threading.Event()

        def callback(msg):
            with lock:
                received_messages.append(msg)
                if len(received_messages) == 2:
                    event.set()

        consumer1.start_consuming(callback)
        consumer2.start_consuming(callback)
        time.sleep(0.3)

        messages_to_send = ["data_package_1", "data_package_2"]
        for message in messages_to_send:
            producer.send(message.encode("utf-8"))

        self.assertTrue(event.wait(MESSAGE_TIMEOUT))
        self.assertEqual(len(received_messages), 2)
        self.assertSetEqual(set(received_messages), {m.encode("utf-8") for m in messages_to_send})

        producer.close()
        consumer1.stop_consuming()
        consumer2.stop_consuming()
        consumer1.close()
        consumer2.close()

        temp_client = MessageMiddlewareQueue(RABBITMQ_HOST, queue_name)
        temp_client.delete()
        temp_client.close()

    def test_exchange_1_to_1(self):
        exchange_name = _unique_name("direct_exchange")
        queue_name = _unique_name("direct_exchange_queue")
        routing_key = "route.key"

        # Ensure the queue is declared before binding
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
        channel = connection.channel()
        channel.queue_declare(queue=queue_name)
        channel.close()
        connection.close()

        producer = MessageMiddlewareExchange(RABBITMQ_HOST, exchange_name, route_keys=[routing_key])
        consumer = MessageMiddlewareExchange(
            RABBITMQ_HOST,
            exchange_name,
            route_keys=[routing_key],
            queue_name=queue_name,
        )

        message_received = []
        event = threading.Event()

        def callback(msg):
            message_received.append(msg)
            event.set()

        consumer.start_consuming(callback)
        time.sleep(0.3)
        producer.send(b"exchange_data")

        self.assertTrue(event.wait(MESSAGE_TIMEOUT))
        self.assertEqual(b"exchange_data", message_received[0])

        producer.close()
        consumer.delete()
        consumer.close()

    def test_exchange_1_to_N_topic(self):
        exchange_name = _unique_name("topic_exchange")

        producer = MessageMiddlewareExchange(
            RABBITMQ_HOST,
            exchange_name,
            route_keys=["topic"],
        )

        consumer_type_a = MessageMiddlewareExchange(
            RABBITMQ_HOST,
            exchange_name,
            route_keys=["topic.type_a.*"],
        )
        consumer_type_b = MessageMiddlewareExchange(
            RABBITMQ_HOST,
            exchange_name,
            route_keys=["topic.type_b.#"],
        )

        type_a_messages = []
        type_b_messages = []
        type_a_event = threading.Event()
        type_b_event = threading.Event()

        def type_a_callback(msg):
            type_a_messages.append(msg)
            type_a_event.set()

        def type_b_callback(msg):
            type_b_messages.append(msg)
            type_b_event.set()

        consumer_type_a.start_consuming(type_a_callback)
        consumer_type_b.start_consuming(type_b_callback)
        time.sleep(0.3)

        type_a_producer = MessageMiddlewareExchange(
            RABBITMQ_HOST,
            exchange_name,
            route_keys=["topic.type_a.sub"],
        )
        type_a_producer.send(b"data_for_type_a")
        self.assertTrue(type_a_event.wait(MESSAGE_TIMEOUT))
        self.assertIn(b"data_for_type_a", type_a_messages)
        self.assertEqual(len(type_b_messages), 0)

        type_b_producer = MessageMiddlewareExchange(
            RABBITMQ_HOST,
            exchange_name,
            route_keys=["topic.type_b.sub.detail"],
        )
        type_b_producer.send(b"data_for_type_b")
        self.assertTrue(type_b_event.wait(MESSAGE_TIMEOUT))
        self.assertIn(b"data_for_type_b", type_b_messages)
        self.assertEqual(len(type_a_messages), 1)

        producer.close()
        type_a_producer.close()
        type_b_producer.close()
        consumer_type_a.delete()
        consumer_type_a.close()
        consumer_type_b.close()

    def test_stop_consuming(self):
        queue_name = _unique_name("stop_demo")
        consumer = MessageMiddlewareQueue(RABBITMQ_HOST, queue_name)

        received_messages = []
        event = threading.Event()

        def callback(msg):
            received_messages.append(msg)
            event.set()

        consumer.start_consuming(callback)
        time.sleep(0.2)
        consumer.stop_consuming()

        producer = MessageMiddlewareQueue(RABBITMQ_HOST, queue_name)
        producer.send(b"should_not_be_received")

        self.assertFalse(event.wait(1.5))
        self.assertEqual(len(received_messages), 0)

        producer.close()
        consumer.delete()
        consumer.close()

    def test_delete_queue_removes_it_from_broker(self):
        queue_name = _unique_name("queue_to_delete")
        client = MessageMiddlewareQueue(RABBITMQ_HOST, queue_name)

        # Ensure the queue exists before attempting to delete it
        client.send(b"test_message")

        client.delete()
        client.close()

        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
        channel = connection.channel()

        with self.assertRaises(pika.exceptions.ChannelClosedByBroker):
            channel.queue_declare(queue=queue_name, passive=True)

        # Check if the channel is open before closing it
        if channel.is_open:
            channel.close()
        connection.close()

    def test_connection_error(self):
        with self.assertRaises(MessageMiddlewareDisconnectedError):
            MessageMiddlewareQueue("non_existent_host_12345", "any_queue")

    def test_queue_send_requires_bytes(self):
        queue_name = _unique_name("queue_requires_bytes")
        client = MessageMiddlewareQueue(RABBITMQ_HOST, queue_name)

        with self.assertRaises(ValueError):
            client.send("not-bytes")

        client.delete()
        client.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
