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
import os
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

from middleware_client import MessageMiddlewareQueue
from protocol import (
    ProtocolError, Opcodes, BatchStatus, DataBatch
)
from protocol.messages import NewTransactions, EOFMessage
from protocol.parsing import write_i32, write_string, write_u8

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

def create_transaction_data_batch(query_id: int, batch_number: int, rows: list[dict]) -> bytes:
    """Creates a proper DataBatch message containing transaction data."""
    
    # Create NewTransactions message
    transactions_msg = NewTransactions()
    transactions_msg.amount = len(rows)
    transactions_msg.batch_number = batch_number
    transactions_msg.batch_status = BatchStatus.CONTINUE
    transactions_msg.rows = []
    
    # Convert dict rows to RawTransaction objects
    from protocol.entities import RawTransaction
    for row in rows:
        transaction = RawTransaction(
            transaction_id=row['transaction_id'],
            store_id=row['store_id'],
            payment_method_id=row['payment_method_id'],
            user_id=row['user_id'],
            original_amount=row['original_amount'],
            discount_applied=row['discount_applied'],
            final_amount=row['final_amount'],
            created_at=row['created_at']
        )
        transactions_msg.rows.append(transaction)
    
    # Serialize the NewTransactions message to bytes
    transactions_bytes = serialize_table_message(transactions_msg)
    
    # Create DataBatch with the embedded message
    data_batch = DataBatch(
        query_ids=[query_id],
        table_ids=[5],  # NEW_TRANSACTION opcode
        batch_bytes=DataBatch.make_embedded(Opcodes.NEW_TRANSACTION, transactions_bytes)
    )
    data_batch.batch_number = batch_number
    data_batch.total_shards = 1
    data_batch.shard_num = 0
    
    return data_batch.to_bytes()

def serialize_table_message(msg) -> bytes:
    """Helper to serialize a table message to bytes."""
    import struct
    
    body_parts = []
    
    # nRows (i32)
    body_parts.append(struct.pack('<I', msg.amount))
    
    # batchNumber (i64) 
    body_parts.append(struct.pack('<Q', msg.batch_number))
    
    # status (u8)
    body_parts.append(struct.pack('<B', msg.batch_status))
    
    # Serialize all rows
    for row in msg.rows:
        # Number of key-value pairs
        row_dict = row.__dict__
        body_parts.append(struct.pack('<I', len(row_dict)))
        
        # Write each key-value pair
        for key, value in row_dict.items():
            key_bytes = key.encode('utf-8')
            body_parts.append(struct.pack('<I', len(key_bytes)))
            body_parts.append(key_bytes)
            
            value_bytes = str(value).encode('utf-8')
            body_parts.append(struct.pack('<I', len(value_bytes)))
            body_parts.append(value_bytes)
    
    return b''.join(body_parts)

def create_eof_message(table_type: str) -> bytes:
    """Creates a proper EOFMessage for a specific table type."""
    eof_msg = EOFMessage()
    eof_msg.create_eof_message(batch_number=0, table_type=table_type)
    return eof_msg.to_bytes()

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

            message_bytes = create_transaction_data_batch(QUERY_ID, batch_num, batch_rows)
            producer.send(message_bytes)
            print(f"Sent Batch #{batch_num} ({len(batch_rows)} records)")
            batch_num += 1
            time.sleep(0.01) # Simulate a small delay between batches

    # 4. Send the final EOF Signal message for transactions table
    total_batches_sent = batch_num - 1  # batch_num is incremented after each batch
    
    eof_signal = create_eof_message("transactions")
    producer.send(eof_signal)
    print(f"Sent EOF Signal for 'transactions' table after {total_batches_sent} batches")
    
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