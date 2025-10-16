import os
import threading
import time
import uuid

import pika
import pytest

from middleware.middleware_client import (
    MessageMiddlewareExchange,
    MessageMiddlewareQueue,
)

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RUN_INTEGRATION = os.getenv("RUN_RABBITMQ_INTEGRATION", "0").lower() in {
    "1",
    "true",
    "yes",
}

if not RUN_INTEGRATION:
    pytest.skip(
        "RabbitMQ integration tests disabled. Set RUN_RABBITMQ_INTEGRATION=1 to enable.",
        allow_module_level=True,
    )

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module", autouse=True)
def rabbitmq_ready():
    deadline = time.time() + 30
    while time.time() < deadline:
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=RABBITMQ_HOST)
            )
            connection.close()
            return
        except pika.exceptions.AMQPConnectionError:
            time.sleep(1)
    pytest.skip(f"RabbitMQ server is not reachable at host '{RABBITMQ_HOST}'")


def test_queue_send_receive_1_to_1():
    queue_name = f"integration-queue-{uuid.uuid4()}"
    message = b"integration-message"

    producer = MessageMiddlewareQueue(RABBITMQ_HOST, queue_name)
    consumer = MessageMiddlewareQueue(RABBITMQ_HOST, queue_name)

    received_messages = []
    event = threading.Event()

    def on_message(body):
        received_messages.append(body)
        event.set()

    try:
        consumer.start_consuming(on_message)
        time.sleep(0.3)
        producer.send(message)

        assert event.wait(timeout=5), "Timed out waiting for message"
        assert received_messages == [message]
    finally:
        consumer.stop_consuming()
        producer.close()
        consumer.delete()
        consumer.close()


def test_exchange_publish_subscribe_1_to_n():
    exchange_name = f"integration-exchange-multi-{uuid.uuid4()}"
    routing_key = f"route.{uuid.uuid4().hex}"
    queue_a = f"integration-exchange-queue-a-{uuid.uuid4()}"
    queue_b = f"integration-exchange-queue-b-{uuid.uuid4()}"
    message_a = b"integration-exchange-msg-a"
    message_b = b"integration-exchange-msg-b"

    producer_a = MessageMiddlewareExchange(
        RABBITMQ_HOST,
        exchange_name,
        route_keys=[routing_key],
    )
    producer_b = MessageMiddlewareExchange(
        RABBITMQ_HOST,
        exchange_name,
        route_keys=[f"{routing_key}.sub"],
    )

    consumer_a = MessageMiddlewareExchange(
        RABBITMQ_HOST,
        exchange_name,
        route_keys=[f"{routing_key}.#"],
        consumer="integration-consumer-a",
        queue_name=queue_a,
    )
    consumer_b = MessageMiddlewareExchange(
        RABBITMQ_HOST,
        exchange_name,
        route_keys=[routing_key],
        consumer="integration-consumer-b",
        queue_name=queue_b,
    )

    received_a = []
    received_b = []
    event_a = threading.Event()
    event_b = threading.Event()

    def on_message_a(body):
        received_a.append(body)
        if len(received_a) == 2:
            event_a.set()

    def on_message_b(body):
        received_b.append(body)
        if len(received_b) == 1:
            event_b.set()

    try:
        consumer_a.start_consuming(on_message_a)
        consumer_b.start_consuming(on_message_b)
        time.sleep(0.3)
        producer_a.send(message_a)
        producer_b.send(message_b)

        assert event_a.wait(timeout=10), "Timed out waiting for exchange pattern messages"
        assert event_b.wait(timeout=10), "Timed out waiting for exchange direct message"
        assert sorted(received_a) == sorted([message_a, message_b])
        assert received_b == [message_a]
    finally:
        consumer_a.stop_consuming()
        consumer_b.stop_consuming()
        producer_a.close()
        producer_b.close()
        consumer_a.delete()
        consumer_a.close()
        consumer_b.delete()
        consumer_b.close()


def test_queue_send_receive_1_to_n():
    queue_name = f"integration-queue-multi-{uuid.uuid4()}"
    messages = {"msg1": b"integration-queue-msg1", "msg2": b"integration-queue-msg2"}

    producer = MessageMiddlewareQueue(RABBITMQ_HOST, queue_name)
    consumer_a = MessageMiddlewareQueue(RABBITMQ_HOST, queue_name)
    consumer_b = MessageMiddlewareQueue(RABBITMQ_HOST, queue_name)

    received = []
    lock = threading.Lock()
    event = threading.Event()

    def on_message(body):
        with lock:
            received.append(body)
            if len(received) == len(messages):
                event.set()

    try:
        consumer_a.start_consuming(on_message)
        consumer_b.start_consuming(on_message)
        time.sleep(0.3)
        producer.send(messages["msg1"])
        producer.send(messages["msg2"])

        assert event.wait(timeout=10), "Timed out waiting for both queue messages"
        assert sorted(received) == sorted(messages.values())
    finally:
        consumer_a.stop_consuming()
        consumer_b.stop_consuming()
        producer.close()
        consumer_a.close()
        consumer_b.close()
        with MessageMiddlewareQueue(RABBITMQ_HOST, queue_name) as cleanup:
            cleanup.delete()


def test_exchange_publish_subscribe_1_to_1():
    exchange_name = f"integration-exchange-{uuid.uuid4()}"
    routing_key = f"route.{uuid.uuid4().hex}"
    queue_name = f"integration-exchange-queue-{uuid.uuid4()}"
    message = b"integration-exchange-message"

    producer = MessageMiddlewareExchange(
        RABBITMQ_HOST,
        exchange_name,
        route_keys=[routing_key],
    )
    consumer = MessageMiddlewareExchange(
        RABBITMQ_HOST,
        exchange_name,
        route_keys=[routing_key],
        consumer="integration-consumer",
        queue_name=queue_name,
    )

    received_messages = []
    event = threading.Event()

    def on_message(body):
        received_messages.append(body)
        event.set()

    try:
        consumer.start_consuming(on_message)
        time.sleep(0.3)
        producer.send(message)

        assert event.wait(timeout=5), "Timed out waiting for exchange message"
        assert received_messages == [message]
    finally:
        consumer.stop_consuming()
        producer.close()
        consumer.delete()
        consumer.close()
