import unittest
import pika
import time
import threading
from middleware_client import MessageMiddlewareQueue, MessageMiddlewareExchange, MessageMiddlewareDisconnectedError

RABBITMQ_HOST = 'localhost'

class TestRabbitMQClient(unittest.TestCase):
    def test_work_queue_1_to_1(self):
        queue_name = "work_queue_single"
        producer = MessageMiddlewareQueue(RABBITMQ_HOST, queue_name)
        consumer = MessageMiddlewareQueue(RABBITMQ_HOST, queue_name)
        
        message_received = None
        event = threading.Event()

        def callback(msg):
            nonlocal message_received
            message_received = msg
            event.set()

        consumer.start_consuming(callback)
        time.sleep(0.5)

        sent_message = "data_package_1"
        producer.send(sent_message.encode('utf-8'))

        event.wait(timeout=2)

        self.assertIsNotNone(message_received)
        self.assertEqual(sent_message.encode('utf-8'), message_received)

        producer.close()
        consumer.delete()
        consumer.close()

    def test_work_queue_1_to_N(self):
        queue_name = "work_queue_multi"
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
        time.sleep(0.5)

        messages_to_send = ["data_package_1", "data_package_2"]
        producer.send(messages_to_send[0].encode('utf-8'))
        producer.send(messages_to_send[1].encode('utf-8'))

        event.wait(timeout=3)

        self.assertEqual(len(received_messages), 2)
        self.assertSetEqual(set(received_messages), set(msg.encode('utf-8') for msg in messages_to_send))

        producer.close()
        consumer1.delete()
        consumer1.close()
        consumer2.close()

    def test_exchange_1_to_1(self):
        exchange_name = "direct_exchange"
        routing_key = "route.key"
        
        producer = MessageMiddlewareExchange(RABBITMQ_HOST, exchange_name, route_keys=[routing_key])
        consumer = MessageMiddlewareExchange(RABBITMQ_HOST, exchange_name, route_keys=[routing_key])
        
        message_received = None
        event = threading.Event()

        def callback(msg):
            nonlocal message_received
            message_received = msg
            event.set()

        consumer.start_consuming(callback)
        time.sleep(0.5)

        sent_message = "exchange_data"
        producer.send(sent_message.encode('utf-8'))

        event.wait(timeout=2)

        self.assertIsNotNone(message_received)
        self.assertEqual(sent_message.encode('utf-8'), message_received)

        producer.close()
        consumer.delete()
        consumer.close()

    def test_exchange_1_to_N_topic(self):
        exchange_name = "topic_exchange"
        
        producer = MessageMiddlewareExchange(RABBITMQ_HOST, exchange_name, route_keys=["topic"])

        consumer_type_a = MessageMiddlewareExchange(RABBITMQ_HOST, exchange_name, route_keys=["topic.type_a.*"])
        consumer_type_b = MessageMiddlewareExchange(RABBITMQ_HOST, exchange_name, route_keys=["topic.type_b.#"])

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
        time.sleep(0.5)

        type_a_producer = MessageMiddlewareExchange(RABBITMQ_HOST, exchange_name, route_keys=["topic.type_a.sub"])
        type_a_producer.send("data_for_type_a".encode('utf-8'))
        type_a_event.wait(timeout=2)
        
        self.assertIn(b"data_for_type_a", type_a_messages)
        self.assertEqual(len(type_b_messages), 0)

        type_b_producer = MessageMiddlewareExchange(RABBITMQ_HOST, exchange_name, route_keys=["topic.type_b.sub.detail"])
        type_b_producer.send("data_for_type_b".encode('utf-8'))
        type_b_event.wait(timeout=2)
        
        self.assertIn(b"data_for_type_b", type_b_messages)
        self.assertEqual(len(type_a_messages), 1)

        producer.close()
        type_a_producer.close()
        type_b_producer.close()
        consumer_type_a.delete()
        consumer_type_a.close()
        consumer_type_b.close()

    def test_stop_consuming(self):
        queue_name = "stop_demo"
        consumer = MessageMiddlewareQueue(RABBITMQ_HOST, queue_name)
        
        received_messages = []
        event = threading.Event()

        def callback(msg):
            received_messages.append(msg)
            event.set()
        
        consumer.start_consuming(callback)
        time.sleep(0.5)
        consumer.stop_consuming()
        time.sleep(1.0)

        producer = MessageMiddlewareQueue(RABBITMQ_HOST, queue_name)
        producer.send("should_not_be_received".encode('utf-8'))
        
        event_triggered = event.wait(timeout=2.0)

        self.assertFalse(event_triggered)
        self.assertEqual(len(received_messages), 0)
        
        producer.close()
        consumer.delete()
        consumer.close()

    def test_delete_queue_removes_it_from_broker(self):
        queue_name = "queue_to_delete"
        client = MessageMiddlewareQueue(RABBITMQ_HOST, queue_name)
        
        client.delete()
        client.close()

        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
        channel = connection.channel()
        
        with self.assertRaises(pika.exceptions.ChannelClosedByBroker):
            channel.queue_declare(queue=queue_name, passive=True)
            
        connection.close()
        
    def test_connection_error(self):
        with self.assertRaises(MessageMiddlewareDisconnectedError):
            MessageMiddlewareQueue("non_existent_host_12345", "any_queue")


if __name__ == '__main__':
    unittest.main(verbosity=2)
