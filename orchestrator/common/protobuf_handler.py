"""
Protobuf message handler for orchestrator.
Handles reading and writing protobuf Envelope messages to/from sockets.
"""

import struct
import logging
from typing import Optional
import socket

# Note: Import generated protobuf modules after running generate_protos.sh
# from protos import envelope_pb2, table_data_pb2, query_request_pb2, eof_message_pb2


class ProtobufMessageReader:
    """Reads protobuf Envelope messages from socket."""
    
    @staticmethod
    def read_envelope(sock: socket.socket):
        """Read a protobuf Envelope message from socket.
        
        Args:
            sock: Socket to read from
            
        Returns:
            Envelope message or None on EOF
            
        Raises:
            ConnectionError: If connection is closed while reading
            Exception: On other read errors
        """
        from protos import envelope_pb2
        
        try:
            # Read message size (4 bytes big-endian)
            size_bytes = sock.recv(4)
            if not size_bytes or len(size_bytes) < 4:
                logging.debug("action: read_envelope | result: eof | size_bytes: %d", len(size_bytes))
                return None
            
            msg_size = struct.unpack('>I', size_bytes)[0]
            logging.debug("action: read_envelope | msg_size: %d", msg_size)
            
            # Read message bytes
            msg_bytes = b''
            while len(msg_bytes) < msg_size:
                chunk = sock.recv(msg_size - len(msg_bytes))
                if not chunk:
                    raise ConnectionError("Connection closed while reading message")
                msg_bytes += chunk
            
            logging.debug("action: read_envelope | bytes_read: %d", len(msg_bytes))
            
            # Deserialize protobuf
            envelope = envelope_pb2.Envelope()
            envelope.ParseFromString(msg_bytes)
            
            logging.debug("action: read_envelope | result: success | type: %s", 
                         envelope_pb2.Envelope.Opcode.Name(envelope.type))
            
            return envelope
            
        except Exception as e:
            logging.error(f"action: read_envelope | result: fail | error: {e}")
            raise


class ProtobufMessageWriter:
    """Writes protobuf Envelope messages to socket."""
    
    @staticmethod
    def write_envelope(sock: socket.socket, envelope) -> None:
        """Write a protobuf Envelope message to socket.
        
        Args:
            sock: Socket to write to
            envelope: Envelope message to send
            
        Raises:
            Exception: On write error
        """
        from protos import envelope_pb2
        
        try:
            # Serialize
            msg_bytes = envelope.SerializeToString()
            
            logging.debug("action: write_envelope | type: %s | size: %d",
                         envelope_pb2.Envelope.Opcode.Name(envelope.type),
                         len(msg_bytes))
            
            # Write size (4 bytes big-endian)
            size_bytes = struct.pack('>I', len(msg_bytes))
            sock.sendall(size_bytes)
            
            # Write message
            sock.sendall(msg_bytes)
            
            logging.debug("action: write_envelope | result: success")
            
        except Exception as e:
            logging.error(f"action: write_envelope | result: fail | error: {e}")
            raise
    
    @staticmethod
    def send_batch_success(sock: socket.socket) -> None:
        """Send BATCH_RECV_SUCCESS response."""
        from protos import envelope_pb2
        
        envelope = envelope_pb2.Envelope()
        envelope.type = envelope_pb2.Envelope.BATCH_RECV_SUrCESS
        
        logging.debug("action: send_batch_success")
        ProtobufMessageWriter.write_envelope(sock, envelope)
    
    @staticmethod
    def send_batch_fail(sock: socket.socket) -> None:
        """Send BATCH_RECV_FAIL response."""
        from protos import envelope_pb2
        
        envelope = envelope_pb2.Envelope()
        envelope.type = envelope_pb2.Envelope.BATCH_RECV_FAIL
        
        logging.error("action: send_batch_fail")
        ProtobufMessageWriter.write_envelope(sock, envelope)


class ProtobufMessageHandler:
    """Handles processing of protobuf messages.
    
    Allows registering processor functions for different envelope types.
    """
    
    def __init__(self):
        """Initialize message handler with empty processor registry."""
        self.processors = {}
        logging.info("action: protobuf_message_handler_initialized")
    
    def register_processor(self, envelope_type: int, processor_func):
        """Register a processor function for an envelope type.
        
        Args:
            envelope_type: envelope_pb2.Envelope.Opcode value
            processor_func: Function(envelope, client_sock) -> bool
                           Should return True if processed successfully
        """
        from protos import envelope_pb2
        
        type_name = envelope_pb2.Envelope.Opcode.Name(envelope_type)
        self.processors[envelope_type] = processor_func
        logging.info("action: register_processor | type: %s (%d)", type_name, envelope_type)
    
    def handle_envelope(self, envelope, client_sock) -> bool:
        """Process an envelope message.
        
        Args:
            envelope: Protobuf Envelope message
            client_sock: Client socket
            
        Returns:
            True if processed successfully, False otherwise
        """
        from protos import envelope_pb2
        
        envelope_type = envelope.type
        type_name = envelope_pb2.Envelope.Opcode.Name(envelope_type)
        
        logging.debug("action: handle_envelope | type: %s", type_name)
        
        processor = self.processors.get(envelope_type)
        if processor:
            try:
                return processor(envelope, client_sock)
            except Exception as e:
                logging.error("action: handle_envelope | type: %s | result: fail | error: %s",
                             type_name, e)
                return False
        else:
            logging.warning("action: handle_envelope | result: unknown_type | type: %s", type_name)
            return False
