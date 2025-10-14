"""
Provides classes for managing network connections and handling client
communications for a server application.
"""

import logging
import socket
import threading
from typing import List, Callable, Optional

from protocol.dispatcher import recv_msg, recv_raw_frame
from protocol.constants import ProtocolError


class ConnectionManager:
    """
    Handles the low-level tasks of managing TCP connections, including listening for,
    accepting, and handling individual client communications in separate threads.
    """

    def __init__(
        self,
        port: int,
        listen_backlog: int,
        disconnect_handler: Optional[Callable[[socket.socket], None]] = None,
    ):
        """
        Initializes the ConnectionManager by creating and binding a server socket.

        Args:
            port: The port number on which the server will listen for connections.
            listen_backlog: The maximum number of queued connections pending acceptance.
        """
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(("", port))
        self._server_socket.listen(listen_backlog)
        self._stop = threading.Event()
        self._threads: List[threading.Thread] = []
        self._disconnect_handler = disconnect_handler

    def set_stop_flag(self):
        """Signals the server to begin the shutdown process by setting an internal stop flag."""
        self._stop.set()

    def close_server_socket(self):
        """Closes the main server listening socket to prevent new connections during shutdown."""
        self._server_socket.close()

    def is_stopped(self) -> bool:
        """
        Checks if the server's stop flag has been set.

        Returns:
            True if the stop flag is set, False otherwise.
        """
        return self._stop.is_set()

    def accept_connection(self) -> socket.socket:
        """
        Blocks until a new client connection is received and accepts it.

        Returns:
            A new socket object representing the connection with the client.

        Raises:
            OSError: If the accept call fails and the server is not in a stopped state.
        """
        logging.info("action: accept_connections | result: in_progress")
        try:
            client_sock, addr = self._server_socket.accept()
            logging.info(f"action: accept_connections | result: success | ip: {addr[0]}")
            return client_sock
        except OSError:
            if self._stop.is_set():
                # This exception is expected during a graceful shutdown
                raise OSError("Server stopped")
            raise

    def create_client_thread(
        self,
        client_sock: socket.socket,
        handshake_handler: Callable,
        data_handler: Callable
    ) -> threading.Thread:
        """Creates a thread that runs the two-phase client handling logic."""
        thread = threading.Thread(
            target=self._handle_client_connection,
            args=(client_sock, handshake_handler, data_handler)
        )
        self._threads.append(thread)
        thread.start()
        return thread

    def join_all_threads(self):
        """
        Waits for all active client handler threads to finish their execution,
        typically during a graceful shutdown.
        """
        for thread in self._threads:
            thread.join()

    def _handle_client_connection(
        self,
        client_sock: socket.socket,
        handshake_handler: Callable,
        data_handler: Callable
    ):
        """
        Handles a client connection in two distinct phases:
        1.  Handshake: Processes the initial CLIENT_HELLO message.
        2.  Data Pumping: Enters a fast loop to read raw frames and pass
            them to the data handler.
        """
        client_id = None
        try:
            # --- PHASE 1: HANDSHAKE ---
            # The handshake handler is responsible for reading the first message.
            # It should return the client_id on success or None on failure.
            client_id = handshake_handler(client_sock)
            if not client_id:
                # Handshake failed, handler should have logged it.
                # The connection will be closed in the finally block.
                return

            # --- PHASE 2: DATA PUMPING ---
            # If the handshake was successful, enter the fast loop.
            while not self._stop.is_set():
                # Use the ultra-lean raw frame receiver. No deserialization here.
                raw_frame = recv_raw_frame(client_sock)
                
                # The data handler's only job is to queue the work.
                # It returns False if the server wants to close the connection.
                if not data_handler(raw_frame, client_id, client_sock):
                    break
        
        except (ProtocolError, EOFError, OSError) as e:
            # Handle any network error during either phase
            logging.error(
                "action: client_connection_error | client_id: %s | error: %s",
                client_id or "N/A", e
            )
        
        finally:
            if self._disconnect_handler is not None:
                try:
                    self._disconnect_handler(client_sock)
                except Exception as exc:
                    logging.warning(
                        "action: client_disconnect_cleanup | result: fail | error: %s",
                        exc,
                    )
            client_sock.close()

class ServerManager:
    """
    Provides a high-level abstraction for running the server, integrating connection
    management with graceful shutdown via OS signal handling.
    """

    def __init__(
        self,
        port: int,
        listen_backlog: int,
        handshake_handler: Callable, # Renamed from message_handler
        data_handler: Callable,     # New handler
        disconnect_handler: Optional[Callable[[socket.socket], None]] = None,
    ):
        self.connection_manager = ConnectionManager(
            port,
            listen_backlog,
            disconnect_handler,
        )
        self.handshake_handler = handshake_handler
        self.data_handler = data_handler

    def setup_signal_handler(self):
        """
        Configures a signal handler for SIGTERM to allow for a graceful server
        shutdown when requested by the operating system or process manager.
        """
        import signal
        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def run(self):
        self.setup_signal_handler()
        while not self.connection_manager.is_stopped():
            try:
                client_sock = self.connection_manager.accept_connection()
                # --- MODIFIED ---
                self.connection_manager.create_client_thread(
                    client_sock, self.handshake_handler, self.data_handler
                )
            except OSError:
                if self.connection_manager.is_stopped():
                    break
                raise
        
        self.connection_manager.join_all_threads()
        logging.shutdown()
        
    def _handle_sigterm(self, *_):
        """
        The callback function executed upon receiving a SIGTERM signal.

        It initiates a graceful shutdown by setting the stop flag and closing the
        main server socket to unblock the accept call in the main loop.
        """
        self.connection_manager.set_stop_flag()
        self.connection_manager.close_server_socket()