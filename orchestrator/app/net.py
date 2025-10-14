# orchestrator/app/net.py
"""
Refactored Orchestrator using a multiprocessing worker pool.
"""

import logging
import os
import multiprocessing
from typing import Optional

from app.results_consumer import ResultsConsumer
from common.network import ResponseHandler, ServerManager
from protocol.constants import Opcodes
from protocol.dispatcher import recv_msg
from app.worker import ProcessingWorker  # Import the new worker class


class Orchestrator:
    """
    Orchestrator that offloads message processing to a pool of worker processes.
    The main process only handles network I/O.
    """

    def __init__(self, port: int, listen_backlog: int):
        """Initialize the orchestrator, worker pool, and task queue."""
        rabbitmq_host = os.getenv("RABBITMQ_HOST", "rabbitmq")
        num_workers = int(os.getenv("PROCESSING_WORKERS", "12"))

        queue_size = num_workers * 10
        self._task_queue = multiprocessing.Queue(maxsize=queue_size)

        # 2. Create and store worker processes
        self._workers = []
        logging.info("action: orchestrator_init | message: starting %d workers", num_workers)
        for _ in range(num_workers):
            worker = ProcessingWorker()
            process = multiprocessing.Process(
                target=worker.run, args=(self._task_queue,)
            )
            self._workers.append(process)

        # Client state management remains in the main process
        self._client_ids: dict[object, str] = {}
        
        # Results consumer and server manager are unchanged
        results_queue = os.getenv("RESULTS_QUEUE", "orchestrator_results_queue")
        self.results_consumer = ResultsConsumer(results_queue, rabbitmq_host)
        self.server_manager = ServerManager(
            port,
            listen_backlog,
            handshake_handler=self._handle_handshake,
            data_handler=self._queue_raw_message,
            disconnect_handler=self._cleanup_client,
        )
        
    def _get_client_id(self, client_sock) -> Optional[str]:
        return self._client_ids.get(client_sock)

    def _register_client(self, client_sock, client_id: str):
        self._client_ids[client_sock] = client_id
        self.results_consumer.register_client(client_sock, client_id)
        logging.info(
            "action: client_registered | client_id: %s | fileno: %s",
            client_id,
            client_sock.fileno(),
        )

    def _cleanup_client(self, client_sock):
        client_id = self._client_ids.pop(client_sock, None)
        self.results_consumer.unregister_client(client_sock)
        logging.info("action: client_cleaned_up | client_id: %s", client_id)

    def _handle_handshake(self, client_sock) -> Optional[str]:
        """
        Handles the first message from a client, which MUST be a CLIENT_HELLO.
        Returns the client_id on success, None on failure.
        """

        msg = recv_msg(client_sock)

        if msg.opcode != Opcodes.CLIENT_HELLO:
            logging.error("action: handshake | result: fail | reason: expected_hello_got_%s", msg.opcode)
            ResponseHandler.send_failure(client_sock)
            return None

        client_id = getattr(msg, "client_id", None)
        if not client_id:
            logging.error("action: handshake | result: fail | reason: missing_client_id")
            ResponseHandler.send_failure(client_sock)
            return None
            
        self._register_client(client_sock, client_id)
        ResponseHandler.send_success(client_sock)
        return client_id
        
    def _queue_raw_message(self, raw_msg_bytes, client_id, client_sock) -> bool:
        """
        The high-performance data handler. Its only job is to put the raw
        message bytes onto the worker queue and send an ACK.
        """
        try:
            self._task_queue.put((raw_msg_bytes, client_id))
            ResponseHandler.send_success(client_sock)
            return True
        except Exception as e:
            logging.error("action: queue_message | result: fail | error: %s", e)
            ResponseHandler.send_failure(client_sock)
            return False

    def run(self):
        """Start the orchestrator, its workers, and the results consumer."""
        self.results_consumer.start()
        
        for process in self._workers:
            process.start()
            
        try:
            self.server_manager.run()
        finally:
            logging.info("action: orchestrator_shutdown | message: stopping services")
            
            for _ in self._workers:
                self._task_queue.put(None)
                
            for process in self._workers:
                process.join()
                
            self.results_consumer.stop()
            logging.info("action: orchestrator_shutdown | result: success")