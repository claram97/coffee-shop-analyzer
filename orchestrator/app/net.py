import logging
import signal
import socket
import threading

from app import protocol


class Orchestrator:
    def __init__(self, port, listen_backlog):
        """Initialize listening socket and concurrency primitives.

        - Creates and binds the TCP listening socket.
        - `_stop` is a process-wide shutdown flag (set by SIGTERM).
        - `_finished` is a Barrier with the expected number of clients/agencies;
          it is used to block FINISHED handlers until all are in.
        - `_threads` keeps track of per-connection worker threads.
        """
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(("", port))
        self._server_socket.listen(listen_backlog)
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []

    def run(self):
        """Main server loop.

        Installs SIGTERM handler, accepts connections until `_stop` is set,
        and spawns one worker thread per client. On shutdown:
        - breaks the accept loop if the listening socket is closed,
        - joins all worker threads,
        - and calls `logging.shutdown()` to flush logs.
        """
        signal.signal(signal.SIGTERM, self.__handle_sigterm)
        while not self._stop.is_set():
            try:
                client_sock = self.__accept_new_connection()
                t = threading.Thread(
                    target=self.__handle_client_connection, args=(client_sock,)
                )
                self._threads.append(t)
                t.start()
            except OSError:
                if self._stop.is_set():
                    break
                raise
        for t in self._threads:
            t.join()
        logging.shutdown()

    def __accept_new_connection(self):
        """Accept a single client connection.

        Blocks in `accept()` until a client connects, logs the remote IP,
        and returns the connected socket.
        """
        logging.info("action: accept_connections | result: in_progress")
        c, addr = self._server_socket.accept()
        logging.info(f"action: accept_connections | result: success | ip: {addr[0]}")
        return c

    def __handle_client_connection(self, client_sock):
        """Per-connection worker.

        Repeatedly receives framed messages (`protocol.recv_msg`), logs them,
        and delegates handling to `__process_msg`. The loop continues until
        `__process_msg` returns False (connection should close), `_stop` is set,
        EOF is reached, or a socket/protocol error occurs. Always closes the
        client socket on exit.
        """
        while not self._stop.is_set():
            msg = None
            try:
                msg = protocol.recv_msg(client_sock)
                addr = client_sock.getpeername()
                logging.info(
                    "action: receive_message | result: success | ip: %s | opcode: %i",
                    addr[0],
                    msg.opcode,
                )
                if not self.__process_msg(msg, client_sock):
                    break
            except protocol.ProtocolError as e:
                logging.error("action: receive_message | result: fail | error: %s", e)
            except EOFError:
                break
            except OSError as e:
                logging.error("action: send_message | result: fail | error: %s", e)
                break
        client_sock.close()

    def __process_msg(self, msg, client_sock) -> bool:
        """Process a decoded message.""" # <-- CORRECCIÓN 2: Docstring simplificado
        if msg.opcode == protocol.Opcodes.NEW_BETS:
            try:
               pass
            except Exception as e:
                protocol.BetsRecvFail().write_to(client_sock)
                logging.error(
                    "action: apuesta_recibida | result: fail | cantidad: %d", msg.amount
                )
                return True
            logging.info(
                "action: apuesta_recibida | result: success | cantidad: %d",
                msg.amount,
            )
            protocol.BetsRecvSuccess().write_to(client_sock)
            return True
            
        if msg.opcode == protocol.Opcodes.FINISHED:
            return False
        
    def __handle_sigterm(self, *_):
        """SIGTERM handler.

        Sets the global stop flag and closes the listening socket to unblock
        `accept()`. Worker threads already running will drain naturally and
        be joined in `run()`.
        """
        self._stop.set()
        self._server_socket.close()
