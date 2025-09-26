"""
Connection management and client handling for the orchestrator.
"""

import logging
import socket
import threading
from typing import List, Callable, Optional

from ..protocol import recv_msg, ProtocolError


class ConnectionManager:
    """Manages TCP connections and client communication."""
    
    def __init__(self, port: int, listen_backlog: int):
        """Initialize connection manager with listening socket.
        
        Args:
            port: Port number to listen on
            listen_backlog: Maximum number of pending connections
        """
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(("", port))
        self._server_socket.listen(listen_backlog)
        self._stop = threading.Event()
        self._threads: List[threading.Thread] = []
        
    def set_stop_flag(self):
        """Set the stop flag to terminate the server."""
        self._stop.set()
        
    def close_server_socket(self):
        """Close the listening socket."""
        self._server_socket.close()
        
    def is_stopped(self) -> bool:
        """Check if server is stopped."""
        return self._stop.is_set()
        
    def accept_connection(self) -> socket.socket:
        """Accept a single client connection.
        
        Returns:
            Connected client socket
            
        Raises:
            OSError: If accept fails and server is not stopped
        """
        logging.info("action: accept_connections | result: in_progress")
        try:
            client_sock, addr = self._server_socket.accept()
            logging.info(f"action: accept_connections | result: success | ip: {addr[0]}")
            return client_sock
        except OSError:
            if self._stop.is_set():
                raise OSError("Server stopped")
            raise
            
    def create_client_thread(self, client_sock: socket.socket, message_handler: Callable) -> threading.Thread:
        """Create and start a thread for handling client connection.
        
        Args:
            client_sock: Connected client socket
            message_handler: Function to handle messages from this client
            
        Returns:
            Created thread
        """
        thread = threading.Thread(
            target=self._handle_client_connection, 
            args=(client_sock, message_handler)
        )
        self._threads.append(thread)
        thread.start()
        return thread
        
    def join_all_threads(self):
        """Wait for all client threads to complete."""
        for thread in self._threads:
            thread.join()
            
    def _handle_client_connection(self, client_sock: socket.socket, message_handler: Callable):
        """Handle messages from a single client connection.
        
        Args:
            client_sock: Client socket
            message_handler: Function to process received messages
        """
        while not self._stop.is_set():
            try:
                msg = recv_msg(client_sock)
                addr = client_sock.getpeername()
                logging.info(
                    "action: receive_message | result: success | ip: %s | opcode: %i",
                    addr[0],
                    msg.opcode,
                )
                
                # Call the message handler - if it returns False, close connection
                if not message_handler(msg, client_sock):
                    break
                    
            except ProtocolError as e:
                logging.error("action: receive_message | result: protocol error | error: %s", e)
                break  # Close connection on protocol errors
            except EOFError:
                break
            except OSError as e:
                logging.error("action: receive_message | result: fail | error: %s", e)
                break
                
        client_sock.close()


class ServerManager:
    """High-level server management with signal handling."""
    
    def __init__(self, port: int, listen_backlog: int, message_handler: Callable):
        """Initialize server manager.
        
        Args:
            port: Port to listen on
            listen_backlog: Maximum pending connections
            message_handler: Function to handle received messages
        """
        self.connection_manager = ConnectionManager(port, listen_backlog)
        self.message_handler = message_handler
        
    def setup_signal_handler(self):
        """Setup SIGTERM handler for graceful shutdown."""
        import signal
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        
    def run(self):
        """Main server loop.
        
        Accepts connections until stopped, spawns worker threads,
        and handles graceful shutdown.
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
                
        # Wait for all threads to complete and shutdown logging
        self.connection_manager.join_all_threads()
        logging.shutdown()
        
    def _handle_sigterm(self, *_):
        """SIGTERM handler for graceful shutdown."""
        self.connection_manager.set_stop_flag()
        self.connection_manager.close_server_socket()