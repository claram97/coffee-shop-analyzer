#!/usr/bin/env python3
import logging
import os
import signal
import sys
import time

import pika
from google.protobuf.message import DecodeError

from protocol2.heartbeat_pb2 import Heartbeat  # asumimos que ya lo generaste

RABBIT_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
HEARTBEAT_QUEUE = os.getenv("HEARTBEAT_QUEUE", "heartbeat_queue")
PREFETCH = int(os.getenv("HEARTBEAT_PREFETCH", "250"))

_LOG_FMT = "%(asctime)s %(levelname)-8s %(message)s"

def init_logging():
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(level=level, format=_LOG_FMT, datefmt="%Y-%m-%d %H:%M:%S")

def build_connection():
    params = pika.ConnectionParameters(
        host=RABBIT_HOST,
        heartbeat=60,
        blocked_connection_timeout=30,
    )
    return pika.BlockingConnection(params)

def ensure_queue(channel):
    channel.queue_declare(queue=HEARTBEAT_QUEUE, durable=True)
    channel.basic_qos(prefetch_count=PREFETCH)

def parse_heartbeat(body: bytes) -> Heartbeat:
    hb = Heartbeat()
    hb.ParseFromString(body)
    return hb

def on_message(channel, method, properties, body):
    try:
        hb = parse_heartbeat(body)
        key = f"{hb.component_type}:{hb.instance_id}"
        logging.debug(
            "action: heartbeat_received | key: %s | ts: %d",
            key,
            hb.timestamp,
        )
        # aquí actualizás tu tabla de latidos, timers, etc.
    except DecodeError as exc:
        logging.warning("action: heartbeat_decode_error | err: %s", exc)
    finally:
        channel.basic_ack(delivery_tag=method.delivery_tag)

def main():
    init_logging()
    logging.info("heartbeat monitor starting (queue=%s host=%s)", HEARTBEAT_QUEUE, RABBIT_HOST)

    while True:
        try:
            with build_connection() as conn:
                channel = conn.channel()
                ensure_queue(channel)
                channel.basic_consume(queue=HEARTBEAT_QUEUE, on_message_callback=on_message)
                channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as exc:
            logging.error("connection lost, retrying in 5s: %s", exc)
            time.sleep(5)
        except KeyboardInterrupt:
            logging.info("shutdown requested by user")
            break
        except Exception as exc:  # pylint: disable=broad-except
            logging.exception("fatal error in monitor loop: %s", exc)
            time.sleep(5)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, lambda *_: sys.exit(0))
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    main()
