import logging
import multiprocessing as mp
from typing import Dict

from protocol.constants import Opcodes

from common.processing import create_filtered_data_batch, message_logger

from middleware.middleware_client import MessageMiddlewareExchange

STATUS_TEXT_MAP = {0: "Continue", 1: "EOF", 2: "Cancel"}


def processing_worker_main(
    task_queue: mp.Queue,
    worker_idx: int,
    host: str,
    exchange_name: str,
    rk_fmt: str,
    num_routers: int,
):
    logging.info(
        "action: worker_start | worker: %d | host: %s | exchange: %s",
        worker_idx,
        host,
        exchange_name,
    )

    publishers: Dict[str, MessageMiddlewareExchange] = {}

    def _publisher_for_rk(rk: str) -> MessageMiddlewareExchange:
        pub = publishers.get(rk)
        if pub is None:
            logging.info(
                "action: worker_create_publisher | worker: %d | exchange: %s | rk: %s",
                worker_idx,
                exchange_name,
                rk,
            )
            pub = MessageMiddlewareExchange(
                host=host,
                exchange_name=exchange_name,
                route_keys=[rk],
            )
            publishers[rk] = pub
        return pub

    while True:
        task = task_queue.get()
        if task is None:
            logging.info("action: worker_stop | worker: %d", worker_idx)
            break

        # Tasks can be either:
        # - (msg_object, client_id)  -> normal processing (existing flow)
        # - ("__eof__", message_bytes, client_id) -> EOF marker with raw bytes
        try:
            if isinstance(task, tuple) and len(task) == 3 and task[0] == "__eof__":
                _, batch_bytes, client_id = task
                # Broadcast EOF to all filter router replicas
                for pid in range(num_routers):
                    rk = rk_fmt.format(pid=pid)
                    _publisher_for_rk(rk).send(batch_bytes)

                logging.info(
                    "action: worker_eof_forwarded | result: success | worker: %d | replicas: %d",
                    worker_idx,
                    num_routers,
                )
                continue

            msg, client_id = task
            setattr(msg, "client_id", client_id)
            status_value = getattr(msg, "batch_status", 0)
            status_text = STATUS_TEXT_MAP.get(status_value, f"Unknown({status_value})")

            message_logger.write_original_message(msg, status_text)

            filtered_batch = create_filtered_data_batch(msg, client_id)
            batch_bytes = filtered_batch.to_bytes()

            batch_number = int(getattr(filtered_batch, "batch_number", 0) or 0)
            pid = 0 if num_routers <= 1 else batch_number % num_routers
            rk = rk_fmt.format(pid=pid)

            _publisher_for_rk(rk).send(batch_bytes)

            message_logger.log_batch_processing_success(msg, status_text)

        except Exception:
            logging.exception(
                "action: worker_process_data | result: fail | worker: %d | opcode: %s",
                worker_idx,
                getattr(msg, "opcode", "unknown"),
            )
