"""
Tests for the leader election module.

These tests verify that the Bully election algorithm works correctly
in various scenarios.
"""

import pytest
import time
import threading
from unittest.mock import Mock, patch, MagicMock
import socket
import struct

from leader_election import (
    start_election_thread,
    ElectionCoordinator,
    NodeState
)
from leader_election.utils import (
    send_election_message,
    send_coordinator_message,
    answer_election_message,
    send_heartbeat_message,
)
from protocol2 import (
    coordinator_message_pb2,
    election_answer_message_pb2,
    election_message_pb2,
    envelope_pb2,
    heartbeat_message_pb2,
)


class TestElectionUtils:
    """Test the low-level election utility functions."""
    
    def test_send_election_message_success(self):
        """Test sending an ELECTION message and receiving ANSWER."""
        # Create a mock server that responds with ANSWER
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind(('127.0.0.1', 0))
        server_socket.listen(1)
        port = server_socket.getsockname()[1]
        
        def server_handler():
            client, _ = server_socket.accept()
            # Read envelope
            length_bytes = client.recv(4)
            msg_len = struct.unpack('<I', length_bytes)[0]
            msg_data = client.recv(msg_len)
            
            # Verify it's an ELECTION message
            envelope = envelope_pb2.Envelope()
            envelope.ParseFromString(msg_data)
            assert envelope.type == envelope_pb2.ELECTION
            
            # Send ANSWER envelope
            answer = election_answer_message_pb2.ElectionAnswer()
            response_envelope = envelope_pb2.Envelope()
            response_envelope.type = envelope_pb2.ELECTION_ANSWER
            response_envelope.election_answeer.CopyFrom(answer)
            serialized = response_envelope.SerializeToString()
            
            client.sendall(struct.pack('<I', len(serialized)))
            client.sendall(serialized)
            client.close()
            
        thread = threading.Thread(target=server_handler, daemon=True)
        thread.start()
        
        # Send election message
        result = send_election_message('127.0.0.1', port, timeout=2.0)
        
        assert result is True
        server_socket.close()
        
    def test_send_election_message_no_response(self):
        """Test sending ELECTION message with no response."""
        # Try to connect to a port that's not listening
        result = send_election_message('127.0.0.1', 9999, timeout=0.5)
        assert result is False
        
    def test_send_coordinator_message_success(self):
        """Test sending a COORDINATOR message."""
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind(('127.0.0.1', 0))
        server_socket.listen(1)
        port = server_socket.getsockname()[1]
        
        received_leader = []
        
        def server_handler():
            client, _ = server_socket.accept()
            # Read envelope
            length_bytes = client.recv(4)
            msg_len = struct.unpack('<I', length_bytes)[0]
            msg_data = client.recv(msg_len)
            
            # Parse envelope
            envelope = envelope_pb2.Envelope()
            envelope.ParseFromString(msg_data)
            assert envelope.type == envelope_pb2.COORDINATOR
            
            received_leader.append(envelope.coordinator.new_leader)
            client.close()
            
        thread = threading.Thread(target=server_handler, daemon=True)
        thread.start()
        
        result = send_coordinator_message('127.0.0.1', port, 42, timeout=2.0)
        
        time.sleep(0.5)
        assert result is True
        assert len(received_leader) == 1
        assert received_leader[0] == 42
        server_socket.close()
    
    def test_send_heartbeat_message_success(self):
        """Test sending a HEARTBEAT message and receiving ACK."""
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind(('127.0.0.1', 0))
        server_socket.listen(1)
        port = server_socket.getsockname()[1]
        
        def server_handler():
            client, _ = server_socket.accept()
            length_bytes = client.recv(4)
            msg_len = struct.unpack('<I', length_bytes)[0]
            msg_data = client.recv(msg_len)
            
            envelope = envelope_pb2.Envelope()
            envelope.ParseFromString(msg_data)
            assert envelope.type == envelope_pb2.HEARTBEAT
            assert envelope.heartbeat.node_id == 7
            
            ack = envelope_pb2.Envelope()
            ack.type = envelope_pb2.HEARTBEAT_ACK
            ack.heartbeat_ack.received_at_ms = 123456
            payload = ack.SerializeToString()
            
            client.sendall(struct.pack('<I', len(payload)))
            client.sendall(payload)
            client.close()
        
        thread = threading.Thread(target=server_handler, daemon=True)
        thread.start()
        
        result = send_heartbeat_message('127.0.0.1', port, node_id=7, timeout=1.0)
        
        assert result is True
        server_socket.close()
    
    def test_send_heartbeat_message_no_response(self):
        """Test heartbeat send failure when leader is unreachable."""
        result = send_heartbeat_message('127.0.0.1', 9998, node_id=3, timeout=0.2)
        assert result is False
        

class TestElectionCoordinator:
    """Test the ElectionCoordinator class."""
    
    def test_coordinator_initialization(self):
        """Test that coordinator initializes correctly."""
        nodes = [(1, '127.0.0.1', 5001), (2, '127.0.0.1', 5002)]
        coordinator = ElectionCoordinator(
            my_id=1,
            my_host='127.0.0.1',
            my_port=5001,
            all_nodes=nodes
        )
        
        assert coordinator.my_id == 1
        assert coordinator.state == NodeState.FOLLOWER
        assert coordinator.current_leader is None
        
    def test_coordinator_start_stop(self):
        """Test starting and stopping the coordinator."""
        nodes = [(1, '127.0.0.1', 5001)]
        coordinator = ElectionCoordinator(
            my_id=1,
            my_host='127.0.0.1',
            my_port=5001,
            all_nodes=nodes
        )
        
        coordinator.start()
        time.sleep(0.5)
        assert coordinator._listener_thread.is_alive()
        
        coordinator.stop()
        time.sleep(0.5)
        assert not coordinator._listener_thread.is_alive()
        
    def test_highest_id_becomes_leader(self):
        """Test that node with highest ID becomes leader when no higher nodes exist."""
        nodes = [(1, '127.0.0.1', 5001), (2, '127.0.0.1', 5002), (3, '127.0.0.1', 5003)]
        
        # Node 3 should become leader immediately (no higher nodes)
        callback_results = []
        def callback(leader_id, am_i_leader):
            callback_results.append((leader_id, am_i_leader))
        
        coordinator = ElectionCoordinator(
            my_id=3,
            my_host='127.0.0.1',
            my_port=5003,
            all_nodes=nodes,
            on_leader_change=callback
        )
        
        coordinator.start()
        time.sleep(0.5)
        
        # Trigger election
        coordinator.start_election()
        time.sleep(1.5)
        
        # Node 3 should be leader
        assert coordinator.am_i_leader()
        assert coordinator.get_current_leader() == 3
        assert len(callback_results) >= 1
        assert callback_results[0] == (3, True)
        
        coordinator.stop()
        
    def test_election_in_progress_flag(self):
        """Test that only one election runs at a time."""
        nodes = [(1, '127.0.0.1', 5001), (2, '127.0.0.1', 5002)]
        
        election_count = []
        original_run_election = ElectionCoordinator._run_election
        
        def slow_run_election(self):
            election_count.append(1)
            time.sleep(1.0)  # Make election slower
            original_run_election(self)
            
        coordinator = ElectionCoordinator(
            my_id=2,
            my_host='127.0.0.1',
            my_port=5002,
            all_nodes=nodes,
            election_timeout=2.0
        )
        
        # Patch to slow down election
        coordinator._run_election = lambda: slow_run_election(coordinator)
        
        coordinator.start()
        time.sleep(0.5)
        
        # Start first election
        coordinator.start_election()
        time.sleep(0.2)
        
        # Try to start second election (should be ignored)
        coordinator.start_election()
        time.sleep(0.2)
        coordinator.start_election()
        
        time.sleep(3.0)
        
        # Only one election should have run
        assert len(election_count) == 1
        
        coordinator.stop()
    
    def test_handle_heartbeat_updates_last_seen(self):
        """Leader should record follower heartbeat and send ACK."""
        nodes = [(1, '127.0.0.1', 5001), (2, '127.0.0.1', 5002)]
        coordinator = ElectionCoordinator(
            my_id=2,
            my_host='127.0.0.1',
            my_port=5002,
            all_nodes=nodes
        )
        coordinator.state = NodeState.LEADER
        coordinator.current_leader = 2
        coordinator._reset_follower_tracking()
        
        class DummySock:
            def __init__(self):
                self.data = b""
            def sendall(self, payload):
                self.data += payload
        
        dummy_sock = DummySock()
        heartbeat = heartbeat_message_pb2.Heartbeat()
        heartbeat.node_id = 1
        heartbeat.sent_at_ms = 1000
        
        coordinator._handle_heartbeat(heartbeat, dummy_sock)
        
        last_seen = coordinator.get_follower_last_seen(1)
        assert last_seen is not None
        
        msg_len = struct.unpack('<I', dummy_sock.data[:4])[0]
        ack_payload = dummy_sock.data[4:4 + msg_len]
        ack_envelope = envelope_pb2.Envelope()
        ack_envelope.ParseFromString(ack_payload)
        assert ack_envelope.type == envelope_pb2.HEARTBEAT_ACK


class TestMultiNodeElection:
    """Test elections with multiple nodes."""
    
    def test_two_nodes_election(self):
        """Test election between two nodes."""
        nodes = [
            (1, '127.0.0.1', 6001),
            (2, '127.0.0.1', 6002)
        ]
        
        results_1 = []
        results_2 = []
        
        def callback_1(leader_id, am_i_leader):
            results_1.append((leader_id, am_i_leader))
            
        def callback_2(leader_id, am_i_leader):
            results_2.append((leader_id, am_i_leader))
        
        # Start both coordinators
        coord1 = ElectionCoordinator(
            my_id=1,
            my_host='127.0.0.1',
            my_port=6001,
            all_nodes=nodes,
            on_leader_change=callback_1,
            election_timeout=2.0
        )
        
        coord2 = ElectionCoordinator(
            my_id=2,
            my_host='127.0.0.1',
            my_port=6002,
            all_nodes=nodes,
            on_leader_change=callback_2,
            election_timeout=2.0
        )
        
        coord1.start()
        coord2.start()
        time.sleep(1.5)
        
        # Node 1 starts election (node 2 should win)
        coord1.start_election()
        time.sleep(5.0)  # Give more time for election to complete
        
        # Node 2 should be leader (higher ID)
        assert coord2.am_i_leader()
        assert coord2.get_current_leader() == 2
        assert coord1.get_current_leader() == 2
        assert not coord1.am_i_leader()
        
        coord1.stop()
        coord2.stop()
        
    def test_three_nodes_election(self):
        """Test election with three nodes."""
        nodes = [
            (1, '127.0.0.1', 7001),
            (2, '127.0.0.1', 7002),
            (3, '127.0.0.1', 7003)
        ]
        
        coordinators = []
        results = [[], [], []]
        
        for i, (node_id, host, port) in enumerate(nodes):
            def make_callback(idx):
                def callback(leader_id, am_i_leader):
                    results[idx].append((leader_id, am_i_leader))
                return callback
            
            coord = ElectionCoordinator(
                my_id=node_id,
                my_host=host,
                my_port=port,
                all_nodes=nodes,
                on_leader_change=make_callback(i),
                election_timeout=2.0
            )
            coord.start()
            coordinators.append(coord)
            
        time.sleep(1.0)
        
        # Node 1 starts election
        coordinators[0].start_election()
        time.sleep(4.0)
        
        # Node 3 should be leader (highest ID)
        assert coordinators[2].am_i_leader()
        assert coordinators[2].get_current_leader() == 3
        assert coordinators[1].get_current_leader() == 3
        assert coordinators[0].get_current_leader() == 3
        
        for coord in coordinators:
            coord.stop()
            
    def test_election_after_leader_failure(self):
        """Test election triggered after leader fails."""
        nodes = [
            (1, '127.0.0.1', 8001),
            (2, '127.0.0.1', 8002),
            (3, '127.0.0.1', 8003)
        ]
        
        results = [[], [], []]
        
        def make_callback(idx):
            def callback(leader_id, am_i_leader):
                results[idx].append((leader_id, am_i_leader))
            return callback
        
        # Start all three nodes
        coord1 = ElectionCoordinator(1, '127.0.0.1', 8001, nodes, make_callback(0), 2.0)
        coord2 = ElectionCoordinator(2, '127.0.0.1', 8002, nodes, make_callback(1), 2.0)
        coord3 = ElectionCoordinator(3, '127.0.0.1', 8003, nodes, make_callback(2), 2.0)
        
        coord1.start()
        coord2.start()
        coord3.start()
        time.sleep(1.0)
        
        # Start election - node 3 becomes leader
        coord1.start_election()
        time.sleep(3.0)
        
        assert coord3.am_i_leader()
        
        # Simulate leader (node 3) failure by stopping it
        coord3.stop()
        time.sleep(1.0)
        
        # Node 2 detects failure and starts election
        coord2.start_election()
        time.sleep(3.0)
        
        # Node 2 should now be leader
        assert coord2.am_i_leader()
        assert coord2.get_current_leader() == 2
        assert coord1.get_current_leader() == 2
        
        coord1.stop()
        coord2.stop()


class TestStartElectionThread:
    """Test the start_election_thread entry point function."""
    
    def test_start_election_thread_creates_coordinator(self):
        """Test that start_election_thread creates and starts a coordinator."""
        nodes = [(1, '127.0.0.1', 9001)]
        
        coordinator = start_election_thread(
            my_id=1,
            my_host='127.0.0.1',
            my_port=9001,
            all_nodes=nodes,
            election_timeout=2.0
        )
        
        time.sleep(0.5)
        
        assert isinstance(coordinator, ElectionCoordinator)
        assert coordinator._listener_thread.is_alive()
        
        coordinator.stop()
        
    def test_callback_on_leader_change(self):
        """Test that callback is invoked when leadership changes."""
        nodes = [(1, '127.0.0.1', 9101)]
        
        callback_invocations = []
        
        def on_leader_change(leader_id, am_i_leader):
            callback_invocations.append({
                'leader_id': leader_id,
                'am_i_leader': am_i_leader
            })
        
        coordinator = start_election_thread(
            my_id=1,
            my_host='127.0.0.1',
            my_port=9101,
            all_nodes=nodes,
            on_leader_change=on_leader_change,
            election_timeout=2.0
        )
        
        time.sleep(0.5)
        coordinator.start_election()
        time.sleep(2.0)
        
        # Should have been called when node became leader
        assert len(callback_invocations) >= 1
        assert callback_invocations[0]['leader_id'] == 1
        assert callback_invocations[0]['am_i_leader'] is True
        
        coordinator.stop()


class TestEdgeCases:
    """Test edge cases and error conditions."""
    
    def test_single_node_becomes_leader_immediately(self):
        """Test that a single node becomes leader immediately."""
        nodes = [(1, '127.0.0.1', 9201)]
        
        coordinator = start_election_thread(
            my_id=1,
            my_host='127.0.0.1',
            my_port=9201,
            all_nodes=nodes,
            election_timeout=1.0
        )
        
        time.sleep(0.5)
        coordinator.start_election()
        time.sleep(1.5)
        
        assert coordinator.am_i_leader()
        assert coordinator.get_current_leader() == 1
        
        coordinator.stop()
        
    def test_concurrent_elections_handled_correctly(self):
        """Test that concurrent elections from multiple nodes are handled."""
        nodes = [
            (1, '127.0.0.1', 9301),
            (2, '127.0.0.1', 9302)
        ]
        
        coord1 = start_election_thread(1, '127.0.0.1', 9301, nodes, election_timeout=2.0)
        coord2 = start_election_thread(2, '127.0.0.1', 9302, nodes, election_timeout=2.0)
        
        time.sleep(1.0)
        
        # Both start elections at the same time
        coord1.start_election()
        coord2.start_election()
        
        time.sleep(4.0)
        
        # Node 2 should eventually be leader
        assert coord2.get_current_leader() == 2
        assert coord2.am_i_leader()
        
        coord1.stop()
        coord2.stop()
        
    def test_election_timeout_triggers_restart(self):
        """Test that election timeout triggers a restart."""
        nodes = [
            (1, '127.0.0.1', 9401),
            (2, '127.0.0.1', 9402)  # Node 2 doesn't exist
        ]
        
        # Node 1 will try to contact node 2, timeout, and become leader
        coordinator = start_election_thread(
            my_id=1,
            my_host='127.0.0.1',
            my_port=9401,
            all_nodes=nodes,
            election_timeout=1.0
        )
        
        time.sleep(0.5)
        coordinator.start_election()
        time.sleep(12.0)  # Wait for timeout and retry
        
        # Node 1 should eventually become leader after timeout
        assert coordinator.get_current_leader() == 1
        assert coordinator.am_i_leader()
        
        coordinator.stop()


if __name__ == '__main__':
    pytest.main([__file__, '-v', '-s'])
