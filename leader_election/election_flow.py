"""
Election flow module for Bully algorithm.

This module provides a thread-based election coordinator that can be used
by any component to participate in leader election. The election logic runs
in a separate thread and can be triggered from any component.

Heartbeat monitoring is handled independently by the component itself.
"""

import socket
import struct
import logging
import threading
import time
import random
from typing import List, Tuple, Callable, Optional, Set
from enum import Enum

from protocol2 import (
    coordinator_message_pb2,
    election_answer_message_pb2,
    election_message_pb2,
    envelope_pb2,
    heartbeat_message_pb2,
    heartbeat_ack_message_pb2,
)
from .utils import (
    send_election_message,
    send_coordinator_message,
    answer_election_message,
    send_heartbeat_message,
)


logger = logging.getLogger(__name__)


class NodeState(Enum):
    """States for a node in the election process."""
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


class ElectionCoordinator:
    """
    Coordinates the Bully election algorithm for a node.
    
    This coordinator runs in a separate thread and handles:
    - Starting elections when triggered
    - Responding to election messages from other nodes
    - Accepting coordinator announcements
    - Managing node state transitions
    
    Heartbeat monitoring is handled externally by the component.
    """
    
    def __init__(self, 
                 my_id: int,
                 my_host: str,
                 my_port: int,
                 all_nodes: List[Tuple[int, str, int]],
                 on_leader_change: Optional[Callable[[int, bool], None]] = None,
                 election_timeout: float = 5.0):
        """
        Initialize the election coordinator.
        
        Args:
            my_id: This node's unique identifier
            my_host: This node's hostname/IP
            my_port: This node's port for election messages
            all_nodes: List of (id, host, port) for all nodes in the cluster
            on_leader_change: Callback when leader changes: (new_leader_id, am_i_leader)
            election_timeout: Timeout for waiting for responses during election
        """
        self.my_id = my_id
        self.my_host = my_host
        self.my_port = my_port
        self.all_nodes = sorted(all_nodes, key=lambda x: x[0])  # Sort by ID
        self.on_leader_change = on_leader_change
        self.election_timeout = election_timeout
        
        # State
        self.current_leader: Optional[int] = None
        self.state = NodeState.FOLLOWER
        self._state_lock = threading.Lock()
        
        # Threading
        self._listener_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._election_in_progress = threading.Event()
        self._heartbeat_lock = threading.Lock()
        self._follower_last_seen = {}
        self._leader_since: Optional[float] = None
        self._missing_followers_logged: Set[int] = set()
        self._stepping_down = False
        
        # Socket for listening to election messages
        self._server_socket: Optional[socket.socket] = None
        
    def start(self):
        """Start the election coordinator listener thread."""
        if self._listener_thread and self._listener_thread.is_alive():
            logger.warning("Election coordinator already started")
            return
            
        self._stop_event.clear()
        
        # Start listener thread for incoming election messages
        self._listener_thread = threading.Thread(
            target=self._listen_for_messages,
            daemon=True,
            name=f"ElectionListener-{self.my_id}"
        )
        self._listener_thread.start()
        
        logger.info(f"Election coordinator started for node {self.my_id}")
        
    def stop(self):
        """Stop the election coordinator threads."""
        self._stop_event.set()
        self._clear_follower_tracking()
        
        if self._server_socket:
            try:
                self._server_socket.close()
            except Exception as e:
                logger.debug(f"Error closing server socket: {e}")
                
        if self._listener_thread:
            self._listener_thread.join(timeout=2.0)
            
        logger.info(f"Election coordinator stopped for node {self.my_id}")
        
    def start_election(self):
        """
        Trigger an election.
        
        This is the main entry point for starting an election. It can be called
        from any component (e.g., from the heartbeat thread) to initiate the 
        election process.
        """
        if self._election_in_progress.is_set():
            logger.debug("Election already in progress")
            return
            
        self._election_in_progress.set()
        
        # Start election in a separate thread
        election_thread = threading.Thread(
            target=self._run_election,
            daemon=True,
            name=f"Election-{self.my_id}"
        )
        election_thread.start()
        
    def _run_election(self):
        """
        Execute the Bully election algorithm.
        
        Algorithm:
        1. Set state to CANDIDATE
        2. Send ELECTION messages to all nodes with higher IDs
        3. Wait for ANSWER messages with timeout
        4. If no ANSWER received, declare self as leader
        5. If ANSWER received, wait for COORDINATOR message
        """
        try:
            logger.info(f"Node {self.my_id} starting election")
            
            with self._state_lock:
                self.state = NodeState.CANDIDATE
            
            # Get all nodes with higher IDs
            higher_nodes = [(nid, host, port) for nid, host, port in self.all_nodes 
                           if nid > self.my_id]
            
            if not higher_nodes:
                # No higher nodes, I am the leader
                self._become_leader()
                return
                
            # Send ELECTION messages to all higher nodes
            responses = []
            for node_id, host, port in higher_nodes:
                logger.debug(f"Sending ELECTION to node {node_id} at {host}:{port}")
                response = send_election_message(host, port, timeout=self.election_timeout)
                responses.append(response)
                
            # Check if any higher node responded
            if any(responses):
                logger.info(f"Node {self.my_id} received ANSWER, waiting for coordinator")
                # Higher node responded, wait for COORDINATOR message
                self._wait_for_coordinator()
            else:
                # No response from higher nodes, become leader
                logger.info(f"Node {self.my_id} received no ANSWER, becoming leader")
                self._become_leader()
                
        except Exception as e:
            logger.error(f"Error during election: {e}", exc_info=True)
        finally:
            self._election_in_progress.clear()
            
    def _become_leader(self):
        """Declare self as leader and announce to all nodes."""
        logger.info(f"Node {self.my_id} becoming leader")
        
        with self._state_lock:
            self.state = NodeState.LEADER
            self.current_leader = self.my_id
        
        self._stepping_down = False
        self._reset_follower_tracking()
        
        # Notify via callback
        if self.on_leader_change:
            try:
                self.on_leader_change(self.my_id, True)
            except Exception as e:
                logger.error(f"Error in leader change callback: {e}")
        
        # Announce to all other nodes
        for node_id, host, port in self.all_nodes:
            if node_id != self.my_id:
                logger.info(f"Sending COORDINATOR to node {node_id} at {host}:{port}")
                result = send_coordinator_message(host, port, self.my_id, timeout=2.0)
                logger.info(f"COORDINATOR send to node {node_id}: {result}")
                
    def _wait_for_coordinator(self):
        """Wait for a COORDINATOR message after receiving ANSWER."""
        # Wait for coordinator announcement with timeout
        start_time = time.monotonic()
        timeout = self.election_timeout * 2
        
        with self._state_lock:
            initial_leader = self.current_leader
            
        while time.monotonic() - start_time < timeout:
            if self._stop_event.is_set():
                return
                
            with self._state_lock:
                if self.current_leader != initial_leader:
                    # Leader changed, coordinator message received
                    logger.info(f"Node {self.my_id} accepted new leader {self.current_leader}")
                    return
                    
            time.sleep(0.1)
            
        # Timeout waiting for coordinator, restart election
        logger.warning(f"Node {self.my_id} timeout waiting for coordinator, restarting election")
        self._election_in_progress.clear()
        self.start_election()
        
    def _listen_for_messages(self):
        """Listen for incoming election messages on a socket."""
        try:
            self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self._server_socket.bind((self.my_host, self.my_port))
            self._server_socket.listen(5)
            self._server_socket.settimeout(1.0)  # Allow periodic checks of stop event
            
            logger.info(f"Node {self.my_id} listening on {self.my_host}:{self.my_port}")
            
            while not self._stop_event.is_set():
                try:
                    client_sock, addr = self._server_socket.accept()
                    # Handle in separate thread to avoid blocking
                    handler = threading.Thread(
                        target=self._handle_client_message,
                        args=(client_sock,),
                        daemon=True
                    )
                    handler.start()
                except socket.timeout:
                    continue
                except Exception as e:
                    if not self._stop_event.is_set():
                        logger.error(f"Error accepting connection: {e}")
                        
        except Exception as e:
            logger.error(f"Error in listener thread: {e}", exc_info=True)
        finally:
            if self._server_socket:
                self._server_socket.close()
                
    def _handle_client_message(self, client_sock: socket.socket):
        """Handle an incoming message from another node."""
        try:
            # Read length prefix
            length_bytes = client_sock.recv(4)
            if len(length_bytes) < 4:
                return
                
            msg_len = struct.unpack('<I', length_bytes)[0]
            msg_data = client_sock.recv(msg_len)
            
            # Parse envelope
            envelope = envelope_pb2.Envelope()
            envelope.ParseFromString(msg_data)
            
            # Handle based on message type
            if envelope.type == envelope_pb2.ELECTION:
                logger.info(f"Node {self.my_id} received ELECTION message")
                self._handle_election(client_sock)
            elif envelope.type == envelope_pb2.COORDINATOR:
                logger.info(f"Node {self.my_id} received COORDINATOR message")
                self._handle_coordinator(envelope.coordinator)
            elif envelope.type == envelope_pb2.HEARTBEAT:
                self._handle_heartbeat(envelope.heartbeat, client_sock)
            elif envelope.type == envelope_pb2.HEARTBEAT_ACK:
                logger.debug("Received HEARTBEAT_ACK message, ignoring in coordinator listener")
            else:
                logger.warning(f"Node {self.my_id} received unknown message type: {envelope.type}")
                
        except Exception as e:
            logger.error(f"Error handling client message: {e}", exc_info=True)
        finally:
            client_sock.close()
            
    def _handle_election(self, client_sock: socket.socket):
        """Handle incoming ELECTION message."""
        # Send ANSWER back
        answer_election_message(client_sock)
        
        # Start own election if not already in progress
        if not self._election_in_progress.is_set():
            logger.info(f"Node {self.my_id} starting election after receiving ELECTION")
            self.start_election()
            
    def _handle_coordinator(self, coordinator_msg: coordinator_message_pb2.Coordinator):
        """Handle incoming COORDINATOR message."""
        new_leader = coordinator_msg.new_leader
        
        with self._state_lock:
            old_leader = self.current_leader
            self.current_leader = new_leader
            self.state = NodeState.FOLLOWER
        
        if new_leader != self.my_id:
            self._clear_follower_tracking()
            
        logger.info(f"Node {self.my_id} accepted new leader: {new_leader}")
        
        # Notify via callback if leader changed
        if old_leader != new_leader and self.on_leader_change:
            try:
                self.on_leader_change(new_leader, new_leader == self.my_id)
            except Exception as e:
                logger.error(f"Error in leader change callback: {e}")
    
    def graceful_resign(self):
        """Notify peers to start an election before shutting down."""
        with self._state_lock:
            is_leader = self.state == NodeState.LEADER
        if not is_leader:
            logger.debug("Node %s is not leader; skipping graceful resign", self.my_id)
            return
        if self._stepping_down:
            return
        self._stepping_down = True
        logger.info("Node %s stepping down gracefully, notifying peers", self.my_id)
        self._log_unseen_followers(force=True)
        for node_id, host, port in self.all_nodes:
            if node_id == self.my_id:
                continue
            result = send_election_message(host, port, timeout=1.0)
            logger.info(
                "Sent step-down election notice to node %s at %s:%s (ack=%s)",
                node_id,
                host,
                port,
                result,
            )
        logger.info("Node %s completed step-down notifications", self.my_id)
    
    def _handle_heartbeat(self, heartbeat_msg: heartbeat_message_pb2.Heartbeat, client_sock: socket.socket):
        """Handle incoming HEARTBEAT message from a follower."""
        sender_id = heartbeat_msg.node_id
        recv_ts = time.time()
        
        if self.state != NodeState.LEADER:
            logger.debug(
                "Node %s received heartbeat while not leader (current leader=%s)",
                self.my_id,
                self.current_leader,
            )
        else:
            with self._heartbeat_lock:
                self._follower_last_seen[sender_id] = recv_ts
        
        self._send_heartbeat_ack(client_sock, recv_ts)
        self._log_unseen_followers()
    
    def _send_heartbeat_ack(self, client_sock: socket.socket, recv_ts: float):
        """Send a HEARTBEAT_ACK envelope back to the heartbeat sender."""
        ack = heartbeat_ack_message_pb2.HeartbeatAck()
        ack.received_at_ms = int(recv_ts * 1000)
        
        envelope = envelope_pb2.Envelope()
        envelope.type = envelope_pb2.HEARTBEAT_ACK
        envelope.heartbeat_ack.CopyFrom(ack)
        payload = envelope.SerializeToString()
        
        try:
            client_sock.sendall(struct.pack('<I', len(payload)))
            client_sock.sendall(payload)
        except socket.error as exc:
            logger.debug(f"Failed to send HEARTBEAT_ACK: {exc}")
    
    def _log_unseen_followers(self, force: bool = False):
        """Log warnings for followers that have not sent heartbeats yet."""
        with self._heartbeat_lock:
            unseen = [
                node_id
                for node_id, last_seen in self._follower_last_seen.items()
                if last_seen is None and node_id not in self._missing_followers_logged
            ]
        if not unseen:
            return
        if not force:
            if self._leader_since is None:
                return
            if time.monotonic() - self._leader_since < self.election_timeout:
                return
        for node_id in unseen:
            logger.warning(
                "No heartbeat received yet from follower %s since leadership change",
                node_id,
            )
            self._missing_followers_logged.add(node_id)
    
    def _reset_follower_tracking(self):
        """Initialize heartbeat tracking for all followers when becoming leader."""
        with self._heartbeat_lock:
            self._follower_last_seen = {
                node_id: None for node_id, _, _ in self.all_nodes if node_id != self.my_id
            }
            self._leader_since = time.monotonic()
            self._missing_followers_logged = set()
    
    def _clear_follower_tracking(self):
        """Clear heartbeat tracking data when stepping down or stopping."""
        with self._heartbeat_lock:
            self._follower_last_seen = {}
            self._leader_since = None
            self._missing_followers_logged = set()
                
    def get_current_leader(self) -> Optional[int]:
        """Get the current leader ID."""
        with self._state_lock:
            return self.current_leader
            
    def am_i_leader(self) -> bool:
        """Check if this node is the current leader."""
        with self._state_lock:
            return self.state == NodeState.LEADER
    
    def get_follower_last_seen(self, follower_id: int):
        """Get the last heartbeat timestamp (seconds) for a follower."""
        with self._heartbeat_lock:
            return self._follower_last_seen.get(follower_id)


class HeartbeatClient:
    """Periodically sends heartbeats to the current leader."""

    def __init__(
        self,
        coordinator: ElectionCoordinator,
        my_id: int,
        all_nodes: List[Tuple[int, str, int]],
        heartbeat_interval: float = 2.0,
        heartbeat_timeout: float = 1.0,
        max_missed_heartbeats: int = 3,
        startup_grace: float = 4.0,
        election_cooldown: float = 5.0,
        cooldown_jitter: float = 1.0,
    ):
        self.coordinator = coordinator
        self.my_id = my_id
        self.node_map = {node_id: (host, port) for node_id, host, port in all_nodes}
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_timeout = heartbeat_timeout
        self.max_missed_heartbeats = max(1, max_missed_heartbeats)
        self.startup_grace = max(0.0, startup_grace)
        self.election_cooldown = max(0.0, election_cooldown)
        self.cooldown_jitter = max(0.0, cooldown_jitter)
        self._missed_heartbeats = 0
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._last_election_request_ts = -float("inf")
        self._active = False
        self._startup_grace_deadline = 0.0
        self._state_lock = threading.Lock()

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run, daemon=True, name=f"HeartbeatClient-{self.my_id}"
        )
        self._thread.start()
        logger.info(
            "Heartbeat client started (interval=%ss, timeout=%ss, max_missed=%s)",
            self.heartbeat_interval,
            self.heartbeat_timeout,
            self.max_missed_heartbeats,
        )

    def stop(self):
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=2.0)
        self.deactivate()

    def activate(self):
        with self._state_lock:
            already_active = self._active
            self._active = True
            self._missed_heartbeats = 0
            self._startup_grace_deadline = time.monotonic() + self.startup_grace
        if not already_active:
            logger.info("Heartbeat client activated for node %s", self.my_id)

    def deactivate(self):
        with self._state_lock:
            was_active = self._active
            self._active = False
            self._missed_heartbeats = 0
        if was_active:
            logger.info("Heartbeat client deactivated for node %s", self.my_id)

    def _run(self):
        while not self._stop_event.is_set():
            try:
                self._perform_heartbeat_check()
            except Exception as exc:
                logger.error("Heartbeat thread error: %s", exc, exc_info=True)
            finally:
                self._stop_event.wait(self.heartbeat_interval)

    def _perform_heartbeat_check(self):
        with self._state_lock:
            active = self._active
            grace_deadline = self._startup_grace_deadline

        if not active:
            self._missed_heartbeats = 0
            return

        leader_id = self.coordinator.get_current_leader()

        if leader_id is None:
            if not self.coordinator.am_i_leader() and time.monotonic() >= grace_deadline:
                self._request_election("no_leader_known")
            self._missed_heartbeats = 0
            return

        if leader_id == self.my_id:
            # I am the leader, no heartbeat needed
            self._missed_heartbeats = 0
            return

        target = self.node_map.get(leader_id)
        if not target:
            logger.warning(
                "Heartbeat client cannot resolve leader %s in node map", leader_id
            )
            return

        host, port = target
        success = send_heartbeat_message(
            target_host=host,
            target_port=port,
            node_id=self.my_id,
            timeout=self.heartbeat_timeout,
        )

        if success:
            self._missed_heartbeats = 0
            logger.debug("Heartbeat acknowledged by leader %s", leader_id)
        else:
            self._missed_heartbeats += 1
            logger.warning(
                "Heartbeat to leader %s failed (miss %s/%s)",
                leader_id,
                self._missed_heartbeats,
                self.max_missed_heartbeats,
            )
            if self._missed_heartbeats >= self.max_missed_heartbeats:
                self._missed_heartbeats = 0
                self._request_election(
                    f"leader_{leader_id}_unresponsive_after_misses"
                )

    def _request_election(self, reason: str) -> bool:
        now = time.monotonic()
        delay = self.election_cooldown + random.uniform(0.0, self.cooldown_jitter)
        elapsed = now - self._last_election_request_ts
        if elapsed < delay:
            remaining = delay - elapsed
            logger.debug(
                "Skipping election request (%s); cooldown %.2fs remaining",
                reason,
                remaining,
            )
            return False
        self._last_election_request_ts = now
        logger.warning("Triggering election (%s)", reason)
        self.coordinator.start_election()
        return True


def start_election_thread(my_id: int,
                         my_host: str,
                         my_port: int,
                         all_nodes: List[Tuple[int, str, int]],
                         on_leader_change: Optional[Callable[[int, bool], None]] = None,
                         election_timeout: float = 5.0) -> ElectionCoordinator:
    """
    Entry point to start election coordination in a separate thread.
    
    This function creates and starts an ElectionCoordinator that runs
    independently in a background thread. Components can call this function
    to participate in leader election.
    
    The component is responsible for:
    - Starting its own heartbeat thread to monitor leader health
    - Calling coordinator.start_election() when leader failure is detected
    
    Args:
        my_id: This node's unique identifier
        my_host: This node's hostname/IP
        my_port: This node's port for election messages
        all_nodes: List of (id, host, port) for all nodes in the cluster
        on_leader_change: Callback when leader changes: (new_leader_id, am_i_leader)
        election_timeout: Timeout for waiting for responses during election
        
    Returns:
        ElectionCoordinator instance that can be used to trigger elections
        or check leader status
        
    Example:
        >>> def on_leader_change(leader_id, am_i_leader):
        ...     if am_i_leader:
        ...         print(f"I am now the leader!")
        ...     else:
        ...         print(f"New leader is {leader_id}")
        ...
        >>> nodes = [(1, "node1", 5001), (2, "node2", 5002), (3, "node3", 5003)]
        >>> coordinator = start_election_thread(2, "node2", 5002, nodes, on_leader_change)
        >>> 
        >>> # In your heartbeat thread:
        >>> # if leader_is_down():
        >>> #     coordinator.start_election()
        >>> 
        >>> # Check leader status
        >>> leader = coordinator.get_current_leader()
        >>> is_leader = coordinator.am_i_leader()
        >>> 
        >>> # Cleanup when done
        >>> coordinator.stop()
    """
    coordinator = ElectionCoordinator(
        my_id=my_id,
        my_host=my_host,
        my_port=my_port,
        all_nodes=all_nodes,
        on_leader_change=on_leader_change,
        election_timeout=election_timeout
    )
    
    coordinator.start()
    return coordinator
