"""
Provides classes for managing network connections and handling client
communications for a server application.
"""

import logging
import socket
import threading
from typing import List, Callable, Optional

from common.protobuf_handler import ProtobufMessageReader


class ConnectionManager:
    """
    Handles the low-level tasks of managing TCP connections, including listening for,
    accepting, and handling individual client communications in separate threads.
    """

    def __init__(self, port: int, listen_backlog: int):
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

    def create_client_thread(self, client_sock: socket.socket, message_handler: Callable) -> threading.Thread:
        """
        Creates, starts, and tracks a new thread to handle communication with a connected client.

        Args:
            client_sock: The socket object for the connected client.
            message_handler: A callable that will be invoked to process messages
                             received from this client.

        Returns:
            The newly created and started thread object.
        """
        thread = threading.Thread(
            target=self._handle_client_connection,
            args=(client_sock, message_handler)
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

    def _handle_client_connection(self, client_sock: socket.socket, message_handler: Callable):
        """
        The target function for a client handler thread.

        Delegates all message handling to the provided handler function,
        which manages its own message reading loop.

        Args:
            client_sock: The client's socket object.
            message_handler: The function to handle the client connection.
        """
        try:
            # Delegate to the message handler (which has its own loop)
            message_handler(client_sock)
        except Exception as e:
            logging.error("action: handle_client_connection | result: fail | error: %s", e)
        finally:
            # Handler is responsible for closing the socket
            pass


class ServerManager:
    """
    Provides a high-level abstraction for running the server, integrating connection
    management with graceful shutdown via OS signal handling.
    """

    def __init__(self, port: int, listen_backlog: int, message_handler: Callable):
        """
        Initializes the server manager.

        Args:
            port: The port number for the server to listen on.
            listen_backlog: The maximum number of pending connections.
            message_handler: The function responsible for processing client messages.
        """
        self.connection_manager = ConnectionManager(port, listen_backlog)
        self.message_handler = message_handler

    def setup_signal_handler(self):
        """
        Configures a signal handler for SIGTERM to allow for a graceful server
        shutdown when requested by the operating system or process manager.
        """
        import signal
        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def run(self):
        """
        Starts the main server loop.

        This method configures signal handling and then continuously accepts new
        connections, delegating each to a new worker thread until a shutdown is initiated.
        """
        self.setup_signal_handler()

        while not self.connection_manager.is_stopped():
            try:
                client_sock = self.connection_manager.accept_connection()
                self.connection_manager.create_client_thread(client_sock, self.message_handler)
            except OSError:
                if self.connection_manager.is_stopped():
                    break
                raise

        # After the loop exits, wait for all threads to complete before shutting down.
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