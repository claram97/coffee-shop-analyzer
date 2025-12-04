import logging
import multiprocessing as mp
from typing import Dict

from middleware.middleware_client import MessageMiddlewareExchange
from common.processing import create_filtered_data_batch_protocol2
from protocol2.envelope_pb2 import Envelope, MessageType
from protocol2.eof_message_pb2 import EOFMessage
from protocol2.clean_up_message_pb2 import CleanUpMessage

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
        # - ("__cleanup__", message_bytes, client_id) -> Cleanup marker with raw bytes
        try:
            if isinstance(task, tuple) and len(task) == 3 and task[0] == "__eof__":
                _, batch_bytes, client_id = task
                # Parse the envelope, add worker ID to trace, and reserialize
                try:
                    env = Envelope()
                    env.ParseFromString(batch_bytes)
                    if env.type == MessageType.EOF_MESSAGE:
                        # Add worker ID to trace field for deduplication
                        # Format: "orch_worker_id"
                        eof = env.eof
                        if eof.trace:
                            # Trace should be empty when worker receives it (fresh from orchestrator)
                            # If it exists, log a warning but still set it to this worker's ID
                            logging.warning(
                                "action: worker_eof_trace_exists | worker: %d | existing_trace: %s",
                                worker_idx,
                                eof.trace,
                            )
                        # Set trace to this worker's ID (each worker has a unique ID)
                        eof.trace = f"orch_{worker_idx}"
                        batch_bytes = env.SerializeToString()
                except Exception as e:
                    logging.warning(
                        "action: worker_eof_trace_update | result: fail | worker: %d | error: %s",
                        worker_idx,
                        e,
                    )
                    # Continue with original bytes if parsing fails
                
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

            if isinstance(task, tuple) and len(task) == 3 and task[0] == "__cleanup__":
                _, batch_bytes, client_id = task
                # Parse the envelope, add worker ID to trace, and reserialize
                try:
                    env = Envelope()
                    env.ParseFromString(batch_bytes)
                    if env.type == MessageType.CLEAN_UP_MESSAGE:
                        # Add worker ID to trace field for deduplication
                        # Format: "orch_worker_id"
                        cleanup = env.clean_up
                        if cleanup.trace:
                            # Trace should be empty when worker receives it (fresh from orchestrator)
                            # If it exists, log a warning but still append this worker's ID
                            logging.warning(
                                "action: worker_cleanup_trace_exists | worker: %d | existing_trace: %s",
                                worker_idx,
                                cleanup.trace,
                            )
                        # Set trace to this worker's ID (each worker has a unique ID)
                        cleanup.trace = f"orch_{worker_idx}"
                        batch_bytes = env.SerializeToString()
                except Exception as e:
                    logging.warning(
                        "action: worker_cleanup_trace_update | result: fail | worker: %d | error: %s",
                        worker_idx,
                        e,
                    )
                    # Continue with original bytes if parsing fails
                
                # Broadcast cleanup to all filter router replicas
                for pid in range(num_routers):
                    rk = rk_fmt.format(pid=pid)
                    _publisher_for_rk(rk).send(batch_bytes)

                logging.info(
                    "action: worker_cleanup_forwarded | result: success | worker: %d | replicas: %d | client_id: %s",
                    worker_idx,
                    num_routers,
                    client_id,
                )
                continue

            msg, client_id = task

            setattr(msg, "client_id", client_id)
            status_value = getattr(msg, "batch_status", 0)
            status_text = STATUS_TEXT_MAP.get(status_value, f"Unknown({status_value})")

            # Build a protocol2 Envelope containing a DataBatch (TableData payload)
            env = create_filtered_data_batch_protocol2(msg, client_id)
            batch_bytes = env.SerializeToString()

            # batch_number is stored in the TableData payload
            batch_number = int(getattr(env.data_batch.payload, "batch_number", 0) or 0)
            pid = 0 if num_routers <= 1 else batch_number % num_routers
            rk = rk_fmt.format(pid=pid)

            _publisher_for_rk(rk).send(batch_bytes)

        except Exception:
            logging.exception(
                "action: worker_process_data | result: fail | worker: %d | opcode: %s",
                worker_idx,
                getattr(msg, "opcode", "unknown"),
            )