import pytest
import threading
import time
import json
import io
import csv
import os
from queue import Queue, Empty

# Add the project root to the Python path to allow imports from other folders
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from middleware_client import MessageMiddlewareQueue
from protocol import (
    Opcodes,
    write_i32,
    write_string,
    write_u8
)

# --- Test Configuration ---
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
ROUTER_INPUT_QUEUE = "test_router_in"
FINISHER_OUTPUT_QUEUE = "test_orchestrator_out"
FINISHER_INPUT_QUEUES = [f"test_finisher_in_{i+1}" for i in range(2)] # Corresponds to 2 finisher nodes

NUM_BATCHES = 100
RECORDS_PER_BATCH = 50
TOTAL_RECORDS = NUM_BATCHES * RECORDS_PER_BATCH
MOCK_DATA_FILE = "mock_transactions.csv"

# --- Helper functions to build test data and binary messages ---

def generate_test_data_csv(filename: str, num_records: int):
    """Creates a mock CSV file with transaction data for the test."""
    print(f"Generating {num_records} records in '{filename}'...")
    header = [
        'transaction_id', 'store_id', 'payment_method_id', 'voucher_id', 'user_id', 
        'original_amount', 'discount_applied', 'final_amount', 'created_at'
    ]
    with open(filename, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        for i in range(num_records):
            writer.writerow({
                'transaction_id': f'tx_{i:04d}',
                'store_id': f's_{(i % 5) + 1}',
                'payment_method_id': f'pm_{(i % 3) + 1}',
                'voucher_id': f'v_{(i % 10) + 1}' if i % 4 == 0 else '',
                'user_id': f'u_{(i % 20) + 1}',
                'original_amount': f'{(i * 1.8):.2f}',
                'discount_applied': f'{(i * 0.3):.2f}' if i % 4 == 0 else '0.00',
                'final_amount': f'{(i * 1.5):.2f}',
                'created_at': '2024-01-15T10:00:00Z'
            })

def create_raw_transaction_payload(rows: list[dict]) -> bytes:
    """Serializes a list of transaction dicts into a TableMessage body."""
    buffer = io.BytesIO()
    write_i32(buffer, len(rows)) # Write number of rows
    for row in rows:
        write_i32(buffer, len(row)) # Write number of key-value pairs
        for key, value in row.items():
            write_string(buffer, key)
            write_string(buffer, str(value))
    return buffer.getvalue()

def create_data_batch_bytes(query_id: int, batch_number: int, inner_opcode: int, inner_body: bytes) -> bytes:
    """Creates the full binary DataBatch message envelope."""
    # First create the inner DataBatch body
    inner_buffer = io.BytesIO()
    # [u8 list: table_ids]
    inner_buffer.write(b'\x01') # Count = 1
    inner_buffer.write(bytes([Opcodes.NEW_TRANSACTION]))
    # [u8 list: query_ids]
    inner_buffer.write(b'\x01') # Count = 1
    inner_buffer.write(bytes([query_id]))
    # [u16 reserved]
    inner_buffer.write(b'\x00\x00')
    # [i64 batch_number]
    inner_buffer.write(int(batch_number).to_bytes(8, 'little', signed=True))
    # [dict<u8,u8> meta]
    inner_buffer.write(b'\x00')
    # [u16 total_shards], [u16 shard_num]
    inner_buffer.write(b'\x01\x00')
    inner_buffer.write(b'\x00\x00')
    # [embedded message]
    inner_buffer.write(bytes([inner_opcode]))
    inner_buffer.write(len(inner_body).to_bytes(4, 'little', signed=True))
    inner_buffer.write(inner_body)
    
    inner_data = inner_buffer.getvalue()
    
    # Now create the outer DataBatch message frame
    outer_buffer = io.BytesIO()
    write_u8(outer_buffer, Opcodes.DATA_BATCH)  # Outer opcode
    write_i32(outer_buffer, len(inner_data))    # Length of inner data
    outer_buffer.write(inner_data)              # Inner data
    
    return outer_buffer.getvalue()

# --- Pytest Fixtures for Setup and Teardown ---

@pytest.fixture(scope="module")
def test_infrastructure():
    """Fixture to set up all necessary queues and test data."""
    # Generate the test data file
    generate_test_data_csv(MOCK_DATA_FILE, TOTAL_RECORDS)
    
    # Declare all queues needed for the test to ensure they exist
    clients = {
        "producer": MessageMiddlewareQueue(RABBITMQ_HOST, ROUTER_INPUT_QUEUE),
        "listener": MessageMiddlewareQueue(RABBITMQ_HOST, FINISHER_OUTPUT_QUEUE)
    }
    finisher_input_clients = [MessageMiddlewareQueue(RABBITMQ_HOST, name) for name in FINISHER_INPUT_QUEUES]
    
    yield clients
    
    # Teardown: delete the queues after the test module runs
    print("\n--- Tearing down test infrastructure ---")
    try:
        clients['producer'].delete()
        clients['listener'].delete()
        for client in finisher_input_clients:
            client.delete()
    except Exception as e:
        print(f"Error during teardown: {e}")
    finally:
        if os.path.exists(MOCK_DATA_FILE):
            os.remove(MOCK_DATA_FILE)

# --- The End-to-End Test ---

def test_full_pipeline_with_large_batch_count(test_infrastructure):
    """
    Tests the entire flow with over 100 batches read from a CSV file.
    It verifies system throughput and correctness with a larger volume of messages.
    """
    producer = test_infrastructure['producer']
    listener = test_infrastructure['listener']
    
    result_queue = Queue()

    # 1. Start a listener in a background thread to wait for the final result
    def result_callback(body):
        result = json.loads(body.decode('utf-8'))
        result_queue.put(result)
        
    listener.start_consuming(result_callback)
    time.sleep(1) # Give the consumer a moment to start

    # 2. Define the test data and query ID
    QUERY_ID = 1
    
    print("\n--- Sending test messages ---")
    
    # 3. Read the CSV and send data in batches
    with open(MOCK_DATA_FILE, 'r') as f:
        reader = csv.DictReader(f)
        batch_num = 1
        
        for i in range(NUM_BATCHES):
            batch_rows = []
            try:
                for _ in range(RECORDS_PER_BATCH):
                    batch_rows.append(next(reader))
            except StopIteration:
                break
            
            if not batch_rows:
                break

            payload = create_raw_transaction_payload(batch_rows)
            message_bytes = create_data_batch_bytes(QUERY_ID, batch_num, Opcodes.NEW_TRANSACTION, payload)
            producer.send(message_bytes)
            print(f"Sent Batch #{batch_num} ({len(batch_rows)} records)")
            batch_num += 1
            time.sleep(0.01) # Simulate a small delay between batches

    # 4. Send the final EOF Signal message
    # The EOF signal should indicate the total number of batches sent
    total_batches_sent = batch_num - 1  # batch_num is incremented after each batch
    
    # Create proper Finished message body with agency_id (4 bytes)
    finished_body = io.BytesIO()
    write_i32(finished_body, 0)  # agency_id = 0 for this test
    finished_payload = finished_body.getvalue()
    
    eof_signal = create_data_batch_bytes(QUERY_ID, total_batches_sent, Opcodes.FINISHED, finished_payload)
    producer.send(eof_signal)
    print(f"Sent EOF Signal indicating {total_batches_sent} total batches were sent")
    
    # 5. Wait for the result from the listener thread
    print("--- Waiting for final consolidated result ---")
    try:
        final_result = result_queue.get(timeout=60) # Generous timeout for 100+ messages
        print(f"Received final result!")
    except Empty:
        pytest.fail("Test timed out waiting for a result from the ResultsFinisher.")
    finally:
        # Clean up the listener
        listener.stop_consuming()
        
    # 6. Assert that the result is correct
    assert final_result is not None
    assert final_result['query_id'] == str(QUERY_ID)
    assert final_result['status'] == 'success'
    
    result_data = final_result['result']
    # For a Q1-style query, the result is a flat list of all transactions
    assert 'transactions' in result_data
    assert len(result_data['transactions']) == TOTAL_RECORDS
    
    # Verify a specific record to ensure data integrity
    transaction_ids = {tx['transaction_id'] for tx in result_data['transactions']}
    assert 'tx_0042' in transaction_ids
    print(f"\nSUCCESS: Received consolidated result with {len(result_data['transactions'])} records as expected.")