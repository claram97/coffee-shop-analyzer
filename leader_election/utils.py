"""
methods for Bully leader election algorithm
"""

import socket
import struct
import logging
import time
from typing import Optional

from protocol2 import (
    coordinator_message_pb2,
    election_answer_message_pb2,
    election_message_pb2,
    envelope_pb2,
    heartbeat_message_pb2,
    heartbeat_ack_message_pb2,
)

logger = logging.getLogger(__name__)


def send_election_message(target_host: str, target_port: int, timeout: float = 2.0) -> bool:
    """
    Send an ELECTION message to a higher-ID process.
    
    Returns True if the target responds with an ANSWER, False otherwise.
    """
    sock = None
    try:
        # Create election message inside envelope
        election_msg = election_message_pb2.Election()
        envelope = envelope_pb2.Envelope()
        envelope.type = envelope_pb2.ELECTION
        envelope.election.CopyFrom(election_msg)
        serialized = envelope.SerializeToString()
        
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        sock.connect((target_host, target_port))
        
        # Send length prefix (4 bytes) + envelope
        msg_len = len(serialized)
        sock.sendall(struct.pack('<I', msg_len))
        sock.sendall(serialized)
        
        # Wait for ANSWER response
        try:
            length_bytes = sock.recv(4)
            if len(length_bytes) < 4:
                return False
                
            response_len = struct.unpack('<I', length_bytes)[0]
            response_data = sock.recv(response_len)
            
            response_envelope = envelope_pb2.Envelope()
            response_envelope.ParseFromString(response_data)
            
            if response_envelope.type == envelope_pb2.ELECTION_ANSWER:
                return True
            return False
        except socket.timeout:
            return False
            
    except (socket.error, ConnectionRefusedError, socket.timeout) as e:
        logger.debug(f"Failed to send election message to {target_host}:{target_port}: {e}")
        return False
    finally:
        if sock:
            try:
                sock.close()
            except:
                pass


def send_coordinator_message(target_host: str, target_port: int, new_leader_id: int, timeout: float = 2.0) -> bool:
    """
    Send a COORDINATOR message announcing the new leader.
    
    Returns True if successfully sent, False otherwise.
    """
    try:
        # Create coordinator message inside envelope
        coordinator_msg = coordinator_message_pb2.Coordinator()
        coordinator_msg.new_leader = new_leader_id
        
        envelope = envelope_pb2.Envelope()
        envelope.type = envelope_pb2.COORDINATOR
        envelope.coordinator.CopyFrom(coordinator_msg)
        serialized = envelope.SerializeToString()
        
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(timeout)
            sock.connect((target_host, target_port))
            
            # Send length prefix (4 bytes) + envelope
            msg_len = len(serialized)
            sock.sendall(struct.pack('<I', msg_len))
            sock.sendall(serialized)
            
            return True
            
    except (socket.error, ConnectionRefusedError, socket.timeout) as e:
        logger.debug(f"Failed to send coordinator message to {target_host}:{target_port}: {e}")
        return False


def send_heartbeat_message(
    target_host: str,
    target_port: int,
    node_id: int,
    sent_at_ms: Optional[int] = None,
    timeout: float = 2.0,
) -> bool:
    """
    Send a HEARTBEAT message to the leader and wait for an ACK.
    
    Returns True if an ACK is received, False otherwise.
    """
    sent_at = sent_at_ms if sent_at_ms is not None else int(time.time() * 1000)
    
    try:
        heartbeat = heartbeat_message_pb2.Heartbeat()
        heartbeat.node_id = node_id
        heartbeat.sent_at_ms = sent_at
        
        envelope = envelope_pb2.Envelope()
        envelope.type = envelope_pb2.HEARTBEAT
        envelope.heartbeat.CopyFrom(heartbeat)
        serialized = envelope.SerializeToString()
        
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(timeout)
            sock.connect((target_host, target_port))
            
            msg_len = len(serialized)
            sock.sendall(struct.pack('<I', msg_len))
            sock.sendall(serialized)
            
            length_bytes = sock.recv(4)
            if len(length_bytes) < 4:
                return False
            
            response_len = struct.unpack('<I', length_bytes)[0]
            response_data = sock.recv(response_len)
            
            response_envelope = envelope_pb2.Envelope()
            response_envelope.ParseFromString(response_data)
            
            if response_envelope.type == envelope_pb2.HEARTBEAT_ACK:
                return True
            return False
    except (socket.error, ConnectionRefusedError, socket.timeout) as e:
        logger.debug(f"Failed to send heartbeat to {target_host}:{target_port}: {e}")
        return False


def handle_election_message(sock: socket.socket, sender_id: int, my_id: int, 
                            higher_processes: list) -> None:
    """
    Handle an incoming ELECTION message.
    
    When receiving an ELECTION message:
    1. Send back an ANSWER message
    2. Start own election process if not already started
    
    Args:
        sock: The socket on which the ELECTION message was received
        sender_id: ID of the process that sent the ELECTION
        my_id: This process's ID
        higher_processes: List of (host, port, id) tuples for processes with higher IDs
    """
    # Send ANSWER message back
    answer_election_message(sock)
    
    # If we have higher ID than sender, we should start our own election
    if my_id > sender_id:
        logger.info(f"Received ELECTION from {sender_id}, starting own election")


def handle_coordinator_message(coordinator_msg: coordinator_message_pb2.Coordinator) -> int:
    """
    Handle an incoming COORDINATOR message.
    
    Accept the new coordinator announced in the message.
    
    Args:
        coordinator_msg: The received COORDINATOR message
        
    Returns:
        The ID of the new leader
    """
    new_leader_id = coordinator_msg.new_leader
    logger.info(f"Accepted new coordinator: {new_leader_id}")
    return new_leader_id


def answer_election_message(sock: socket.socket) -> None:
    """
    Send an ANSWER message in response to an ELECTION message.
    
    Args:
        sock: The socket to send the ANSWER on
    """
    try:
        # Create election answer inside envelope
        answer = election_answer_message_pb2.ElectionAnswer()
        
        envelope = envelope_pb2.Envelope()
        envelope.type = envelope_pb2.ELECTION_ANSWER
        envelope.election_answeer.CopyFrom(answer)
        serialized = envelope.SerializeToString()
        
        # Send length prefix (4 bytes) + envelope
        msg_len = len(serialized)
        sock.sendall(struct.pack('<I', msg_len))
        sock.sendall(serialized)
        
        # Give the other side time to receive
        sock.shutdown(socket.SHUT_WR)
        
    except socket.error as e:
        logger.debug(f"Failed to send ANSWER message: {e}")
