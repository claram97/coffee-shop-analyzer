# orchestrator/app/net.py
"""
Refactored Orchestrator using modular architecture.
"""

import logging
import multiprocessing as mp
import os
import queue
import time
import threading
from typing import Optional

from app.results_consumer import ResultsConsumer
from common.network import MessageHandler, ResponseHandler, ServerManager
from common.processing import create_filtered_data_batch, message_logger

from middleware.middleware_client import MessageMiddlewareExchange
from protocol.constants import Opcodes
from protocol.messages import Finished

from .worker_pool import processing_worker_main


class Orchestrator:
    """Orchestrator using modular network and processing components."""

    def __init__(self, port: int, listen_backlog: int):
        """Initialize modular orchestrator.

        Args:
            port: Port to listen on
            listen_backlog: Maximum pending connections
        """
        rabbitmq_host = os.getenv("RABBITMQ_HOST", "rabbitmq")
        self._fr_exchange = os.getenv("ORCH_TO_FR_EXCHANGE", "orch.to.fr")
        self._fr_rk_fmt = os.getenv("ORCH_TO_FR_RK_FMT", "fr.{pid:02d}")
        self._num_routers = max(1, int(os.getenv("FILTER_ROUTER_COUNT", "1")))

        self._publishers: dict[str, MessageMiddlewareExchange] = {}

        self.message_handler = MessageHandler()
        self._client_ids: dict[object, str] = {}

        # Per-client state: {client_id: {"finished": bool, "queries_sent": int, "sock": socket}}
        self._client_states: dict[str, dict] = {}

        self._client_ids_lock = threading.RLock()
        self._client_states_lock = threading.RLock()

        results_queue = os.getenv("RESULTS_QUEUE", "orchestrator_results_queue")
        self.results_consumer = ResultsConsumer(results_queue, rabbitmq_host)

        self.server_manager = ServerManager(
            port,
            listen_backlog,
            self._handle_message,
            self._cleanup_client,
        )

        self._host = rabbitmq_host

        self._mp_context = mp.get_context("spawn")
        self._queue_maxsize = max(1, int(os.getenv("ORCH_PROCESS_QUEUE_SIZE", "128")))
        worker_default = os.getenv("ORCH_PROCESS_COUNT") or str(self._num_routers)
        self._worker_count = max(1, int(worker_default))
        # Queue put timeout (seconds). If not configured, use a sensible
        # default so the orchestrator doesn't retry indefinitely by default.
        queue_timeout_raw = os.getenv("ORCH_PROCESS_QUEUE_TIMEOUT")
        default_timeout = 5.0
        self._queue_put_timeout = default_timeout
        if queue_timeout_raw:
            try:
                self._queue_put_timeout = float(queue_timeout_raw)
            except ValueError:
                logging.warning(
                    "action: queue_timeout_parse | result: fail | value: %s | using_default: %s",
                    queue_timeout_raw,
                    default_timeout,
                )
        # Use one multiprocessing.Queue per worker so we can route messages
        # to individual workers (one-queue-per-worker). Queues are created
        # when starting worker processes.
        self._task_queues: list[mp.Queue] = []
        # Round-robin pointer and lock for thread-safe assignment
        self._rr_lock = threading.Lock()
        self._next_worker_idx = 0
        self._workers: list[mp.Process] = []

        self._setup_message_processors()
        self._start_worker_processes()

    def _get_client_id(self, client_sock) -> Optional[str]:
        with self._client_ids_lock:
            return self._client_ids.get(client_sock)

    def _register_client(self, client_sock, client_id: str):
        with self._client_ids_lock:
            self._client_ids[client_sock] = client_id
        with self._client_states_lock:
            self._client_states[client_id] = {"finished": False, "queries_sent": 0, "sock": client_sock}
        self.results_consumer.register_client(client_sock, client_id)

    def _cleanup_client(self, client_sock):
        with self._client_ids_lock:
            client_id = self._client_ids.pop(client_sock, None)
        self.results_consumer.unregister_client(client_sock)
        with self._client_states_lock:
            if client_id and client_id in self._client_states:
                self._client_states.pop(client_id)

    def _publisher_for_rk(self, rk: str) -> MessageMiddlewareExchange:
        pub = self._publishers.get(rk)
        if pub is None:
            logging.info(
                "action: create_publisher | exchange: %s | rk: %s | host: %s",
                self._fr_exchange,
                rk,
                self._host,
            )
            pub = MessageMiddlewareExchange(
                host=self._host,
                exchange_name=self._fr_exchange,
                route_keys=[rk],
            )
            self._publishers[rk] = pub
        return pub

    def _send_to_filter_router_exchange(self, raw: bytes, batch_number: Optional[int]):
        pid = 0 if batch_number is None else int(batch_number) % self._num_routers
        rk = self._fr_rk_fmt.format(pid=pid)
        self._publisher_for_rk(rk).send(raw)

    def _broadcast_eof_to_all(self, raw: bytes):
        for pid in range(self._num_routers):
            rk = self._fr_rk_fmt.format(pid=pid)
            self._publisher_for_rk(rk).send(raw)

    def _setup_message_processors(self):
        """Setup message processors for different message types."""
        for opcode in [
            Opcodes.NEW_MENU_ITEMS,
            Opcodes.NEW_STORES,
            Opcodes.NEW_TRANSACTION_ITEMS,
            Opcodes.NEW_TRANSACTION,
            Opcodes.NEW_USERS,
        ]:
            self.message_handler.register_processor(opcode, self._process_data_message)

        self.message_handler.register_processor(Opcodes.EOF, self._process_eof_message)

        # for opcode in range(1, 5):
        #     self.message_handler.register_processor(opcode, self._process_query_request)

    def _handle_message(self, msg, client_sock) -> bool:
        if msg.opcode == Opcodes.CLIENT_HELLO:
            client_id = getattr(msg, "client_id", None)
            if not client_id:
                logging.error("action: client_hello | result: fail | reason: missing_client_id")
                ResponseHandler.send_failure(client_sock)
                return False

            self._register_client(client_sock, client_id)
            logging.info(
                "action: client_hello | result: success | client_id: %s | fileno: %s",
                client_id,
                client_sock.fileno(),
            )
            ResponseHandler.send_success(client_sock)
            return True

        client_id = self._get_client_id(client_sock)
        if not client_id:
            logging.error(
                "action: message_received | result: fail | reason: missing_handshake | opcode: %s",
                msg.opcode,
            )
            ResponseHandler.send_failure(client_sock)
            return False

        setattr(msg, "client_id", client_id)

        # Track 'finished' message
        if msg.opcode == Opcodes.FINISHED:
            logging.info("Client finished received in net.py")
            with self._client_states_lock:
                state = self._client_states.get(client_id)
                if state:
                    state["finished"] = True
                    self._check_and_close_client(client_id)
            logging.info("action: client_finished | result: received | client_id: %s", client_id)
            return True

        # Track queries sent (simulate: increment when sending a query)
        # You should call self._increment_queries_sent(client_id) wherever you send a query to the client

        return self.message_handler.handle_message(msg, client_sock, client_id=client_id)

    def increment_queries_sent(self, client_id: str):
        with self._client_states_lock:
            state = self._client_states.get(client_id)
            if state:
                state["queries_sent"] += 1
                self._check_and_close_client(client_id)

    def _check_and_close_client(self, client_id: str):
        with self._client_states_lock:
            state = self._client_states.get(client_id)
            if state and state["finished"] and state["queries_sent"] >= 4:
                sock = state["sock"]
                logging.info("action: close_client | result: closing | client_id: %s", client_id)
                try:
                    sock.sendall(Finished().to_bytes())
                    sock.close()
                except Exception:
                    logging.exception("action: close_client | result: fail | client_id: %s", client_id)
                self.results_consumer.unregister_client(sock)
                self._client_states.pop(client_id)
                with self._client_ids_lock:
                    self._client_ids.pop(sock, None)

    def _cleanup_all_clients(self):
        """Clean up all remaining clients on shutdown."""
        with self._client_states_lock:
            for client_id, state in list(self._client_states.items()):
                sock = state["sock"]
                try:
                    sock.close()
                except Exception:
                    pass
                self.results_consumer.unregister_client(sock)
            self._client_states.clear()
        with self._client_ids_lock:
            for sock in list(self._client_ids.keys()):
                try:
                    sock.close()
                except Exception:
                    pass
                self.results_consumer.unregister_client(sock)
            self._client_ids.clear()

    def _start_worker_processes(self):
        if self._worker_count <= 0:
            return

        logging.info(
            "action: start_worker_pool | workers: %d | queue_maxsize: %d",
            self._worker_count,
            self._queue_maxsize,
        )

        for worker_idx in range(self._worker_count):
            q = self._mp_context.Queue(maxsize=self._queue_maxsize)
            self._task_queues.append(q)

            proc = self._mp_context.Process(
                target=processing_worker_main,
                name=f"orch-worker-{worker_idx}",
                args=(
                    q,
                    worker_idx,
                    self._host,
                    self._fr_exchange,
                    self._fr_rk_fmt,
                    self._num_routers,
                ),
                daemon=True,
            )
            proc.start()
            self._workers.append(proc)

    def _stop_worker_processes(self):
        if not getattr(self, "_workers", None):
            return

        # Put sentinel None into each worker queue to signal shutdown
        for q in getattr(self, "_task_queues", []):
            try:
                q.put(None)
            except Exception:
                logging.exception("action: stop_worker_processes | result: put_sentinel_failed")

        for proc in self._workers:
            proc.join(timeout=5)
            if proc.is_alive():
                logging.warning(
                    "action: stop_worker_processes | result: terminate | worker: %s",
                    proc.name,
                )
                proc.terminate()
                proc.join(timeout=1)

        self._workers.clear()

        # Close/join all task queues
        try:
            for q in getattr(self, "_task_queues", []):
                try:
                    q.close()
                    q.join_thread()
                except Exception:
                    logging.debug("action: stop_worker_processes | result: queue_close_skip | queue_idx: %s", getattr(q, "_reader", "unknown"))
        except Exception:
            logging.debug("action: stop_worker_processes | result: queue_close_skip_all")

    def _enqueue_processing_task(self, msg, client_id: str):
        # Try to assign to worker queues using round-robin. If the chosen
        # queue is full, try the next one. If all are full, wait and retry
        # until a queue accepts the task or the optional timeout elapses.
        if not getattr(self, "_task_queues", None):
            raise RuntimeError("no task queues available for workers")

        start_time = time.time()
        deadline = None if self._queue_put_timeout is None else start_time + self._queue_put_timeout
        attempts = 0
        num_queues = len(self._task_queues)

        while True:
            with self._rr_lock:
                start_idx = self._next_worker_idx

            for i in range(num_queues):
                idx = (start_idx + i) % num_queues
                q = self._task_queues[idx]
                try:
                    q.put((msg, client_id), block=False)
                    # Advance round-robin pointer to next worker after the one we used
                    with self._rr_lock:
                        self._next_worker_idx = (idx + 1) % num_queues
                    return
                except queue.Full:
                    # Try the next queue
                    continue

            # No queue had space this iteration
            attempts += 1
            logging.warning(
                "action: enqueue_data_batch | result: retry | reason: all_queues_full | attempts: %d | batch_number: %d | opcode: %d",
                attempts,
                getattr(msg, "batch_number", 0),
                getattr(msg, "opcode", -1),
            )

            if deadline is not None and time.time() >= deadline:
                # Respect configured timeout: raise to let caller respond with failure
                raise queue.Full()

            time.sleep(0.1)

    def _process_data_message(self, msg, client_sock) -> bool:
        try:
            client_id = getattr(msg, "client_id", None)
            if not client_id:
                raise RuntimeError("missing client_id for data message")

            self._enqueue_processing_task(msg, client_id)

            ResponseHandler.send_success(client_sock)
            return True
        except queue.Full:
            ResponseHandler.send_failure(client_sock)
            return False
        except Exception as e:
            return ResponseHandler.handle_processing_error(msg, client_sock, e)

    def _process_filtered_batch(self, msg, status_text: str):
        """Filtra, empaqueta y publica el DataBatch al exchange del Filter Router."""
        try:
            client_id = getattr(msg, "client_id", None)
            if not client_id:
                raise RuntimeError("missing client_id for filtered batch")

            filtered_batch = create_filtered_data_batch(msg, client_id)
            batch_bytes = filtered_batch.to_bytes()

            bn = int(getattr(filtered_batch, "batch_number", 0) or 0)
            self._send_to_filter_router_exchange(batch_bytes, bn)

        except Exception as filter_error:
            logging.error(
                "action: batch_filter | result: fail | batch_number: %d | opcode: %d | error: %s",
                getattr(msg, "batch_number", 0),
                msg.opcode,
                str(filter_error),
            )

    def _process_eof_message(self, msg, client_sock) -> bool:
        """Reenvía EOFs al exchange del Filter Router (broadcast a todas las réplicas)."""
        try:
            table_type = msg.get_table_type()
            logging.info(
                "action: eof_received | result: success | table_type: %s | batch_number: %d",
                table_type,
                getattr(msg, "batch_number", 0),
            )

            message_bytes = msg.to_bytes()

            self._broadcast_eof_to_all(message_bytes)

            logging.info(
                "action: eof_forwarded | result: success | table_type: %s | bytes_length: %d | replicas: %d",
                table_type,
                len(message_bytes),
                self._num_routers,
            )

            ResponseHandler.send_success(client_sock)
            return True

        except Exception as e:
            logging.error(
                "action: eof_processing | result: fail | table_type: %s | batch_number: %d | error: %s",
                getattr(msg, "table_type", "unknown"),
                getattr(msg, "batch_number", 0),
                str(e),
            )
            return ResponseHandler.handle_processing_error(msg, client_sock, e)

    def run(self):
        """Start the orchestrator server and results consumer."""
        self.results_consumer.set_orchestrator(self)
        self.results_consumer.start()
        try:
            self.server_manager.run()
        finally:
            self._stop_worker_processes()
            self.results_consumer.stop()
            self._cleanup_all_clients()


class MockFilterRouterQueue:
    """(Sigue ahí por compat de tests que lo importan en otro lado)"""

    def send(self, message_bytes: bytes):
        logging.info(
            "action: mock_queue_send | result: success | message_size: %d bytes",
            len(message_bytes),
        )

