import pytest
import threading
import time
import json
import os
from queue import Queue, Empty
import pandas as pd
import io

# Add the project root to the Python path
import sys
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

from middleware_client import MessageMiddlewareQueue
from protocol import DataBatch, BatchStatus, Opcodes
from protocol.messages import (
    NewTransactions, NewStores, NewUsers, NewMenuItems, NewTransactionItems,
    NewTransactionStores, NewTransactionItemsMenuItems, NewTransactionStoresUsers
)
from protocol.parsing import write_i32, write_string, write_u8
from constants import QueryType

# --- Test Configuration & Mock Data ---
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
ROUTER_INPUT_QUEUE = "test_router_in"
FINISHER_OUTPUT_QUEUE = "test_orchestrator_out"
FINISHER_INPUT_QUEUES = [f"test_finisher_in_{i+1}" for i in range(2)]

class MockJoinedRow:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
    def __repr__(self):
        return f"<MockJoinedRow {self.__dict__}>"

def generate_mock_data():
    """Creates a dictionary of pandas DataFrames with predictable test data."""
    data = {
        'stores': pd.DataFrame([
            {'store_id': 1, 'store_name': 'Downtown', 'city': 'Testville', 'state': 'CA'},
            {'store_id': 8, 'store_name': 'Uptown', 'city': 'Testville', 'state': 'CA'},
        ]),
        'users': pd.DataFrame([
            {'user_id': 101, 'birthdate': '1990-05-15', 'gender': 'M', 'registered_at': '...'},
            {'user_id': 102, 'birthdate': '1985-11-20', 'gender': 'F', 'registered_at': '...'},
            {'user_id': 103, 'birthdate': '2000-01-30', 'gender': 'O', 'registered_at': '...'},
        ]),
        'menu_items': pd.DataFrame([
            {'item_id': 1, 'item_name': 'Latte', 'category': 'coffee', 'price': 5.0},
            {'item_id': 2, 'item_name': 'Espresso', 'category': 'coffee', 'price': 3.0},
        ]),
        'transactions': pd.DataFrame([
            {'transaction_id': 't1', 'store_id': 1, 'user_id': 101, 'final_amount': 80.0, 'created_at': '2024-01-15T10:00:00Z'},
            {'transaction_id': 't2', 'store_id': 8, 'user_id': 102, 'final_amount': 74.9, 'created_at': '2024-02-20T11:00:00Z'},
            {'transaction_id': 't3', 'store_id': 1, 'user_id': 103, 'final_amount': 90.0, 'created_at': '2024-03-10T05:59:59Z'},
            {'transaction_id': 't4', 'store_id': 1, 'user_id': 101, 'final_amount': 100.0, 'created_at': '2024-04-01T12:00:00Z'},
            {'transaction_id': 't5', 'store_id': 8, 'user_id': 102, 'final_amount': 200.0, 'created_at': '2024-08-01T12:00:00Z'},
            {'transaction_id': 't6', 'store_id': 1, 'user_id': 102, 'final_amount': 10.0, 'created_at': '2024-01-01T12:00:00Z'},
            {'transaction_id': 't7', 'store_id': 1, 'user_id': 102, 'final_amount': 10.0, 'created_at': '2024-01-02T12:00:00Z'},
            {'transaction_id': 't8', 'store_id': 1, 'user_id': 103, 'final_amount': 10.0, 'created_at': '2024-01-03T12:00:00Z'},
        ]),
        'transaction_items': pd.DataFrame([
            {'transaction_id': 'ti1', 'item_id': 1, 'quantity': 5, 'subtotal': 25.0, 'created_at': '2024-01-10T10:00:00Z'},
            {'transaction_id': 'ti2', 'item_id': 2, 'quantity': 10, 'subtotal': 30.0, 'created_at': '2024-01-12T10:00:00Z'},
            {'transaction_id': 'ti3', 'item_id': 1, 'quantity': 2, 'subtotal': 10.0, 'created_at': '2024-02-05T10:00:00Z'},
        ]),
    }
    # Add dummy columns for protocol compliance
    for col in ['payment_method_id', 'original_amount', 'discount_applied']:
        data['transactions'][col] = ''
    for col in ['unit_price']:
        data['transaction_items'][col] = 0.0
    return data

# --- Protocol Helper ---
def create_data_batch(query_id, TableMsgClass, rows, batch_num, is_eof=False, table_ids=None, shard_num=0, total_shards=0):
    """
    FIXED: Serializes ALL fields from the row object, not just the base fields.
    """
    table_msg = TableMsgClass()
    table_msg.amount = len(rows)
    table_msg.batch_number = batch_num
    table_msg.batch_status = BatchStatus.EOF if is_eof else BatchStatus.CONTINUE
    table_msg.rows = [MockJoinedRow(**row) for row in rows]
    
    body_buf = bytearray()
    
    write_i32(body_buf, table_msg.amount)
    body_buf.extend(int(table_msg.batch_number).to_bytes(8, "little", signed=True))
    write_u8(body_buf, table_msg.batch_status)
    for row in table_msg.rows:
        # FIX: Iterate over the row's actual attributes to include joined fields
        row_data = {k: v for k, v in row.__dict__.items() if not k.startswith('_')}
        write_i32(body_buf, len(row_data))
        for key, value in row_data.items():
            write_string(body_buf, key)
            write_string(body_buf, str(value))
    
    if table_ids is None:
        table_ids = [table_msg.opcode]
    
    batch = DataBatch(
        query_ids=[query_id],
        meta={},
        table_ids=table_ids,
        batch_bytes=DataBatch.make_embedded(table_msg.opcode, bytes(body_buf)),
        total_shards=total_shards,
        shard_num=shard_num
    )
    batch.batch_number = batch_num
    return batch.to_bytes()

# --- Pytest Fixtures ---
@pytest.fixture(scope="module")
def rabbitmq_setup():
    all_queues = [ROUTER_INPUT_QUEUE, FINISHER_OUTPUT_QUEUE] + FINISHER_INPUT_QUEUES
    clients = [MessageMiddlewareQueue(RABBITMQ_HOST, name) for name in all_queues]
    print("\n--- Setting up test queues ---")
    yield
    print("\n--- Tearing down test queues ---")
    for client in clients:
        try: client.delete()
        except Exception as e: print(f"Could not delete queue {client.queue_name}: {e}")

@pytest.fixture
def producer_and_listener(rabbitmq_setup):
    producer = MessageMiddlewareQueue(RABBITMQ_HOST, ROUTER_INPUT_QUEUE)
    listener = MessageMiddlewareQueue(RABBITMQ_HOST, FINISHER_OUTPUT_QUEUE)
    result_queue = Queue()
    def callback(body): result_queue.put(json.loads(body.decode('utf-8')))
    listener.start_consuming(callback)
    yield producer, listener, result_queue
    listener.stop_consuming()

# --- Test Class ---
class TestFullPipeline:
    mock_data = generate_mock_data()

    def test_query_1_simple_transactions(self, producer_and_listener):
        producer, _, result_queue = producer_and_listener
        QUERY_ID = 1
        tx_data = self.mock_data['transactions'].to_dict('records')
        msg = create_data_batch(QUERY_ID, NewTransactions, tx_data, batch_num=1, is_eof=True)
        producer.send(msg)
        result = result_queue.get(timeout=10)

        assert result['query_id'] == str(QUERY_ID)
        assert result['status'] == 'success'
        transactions = result['result']['transactions']
        
        # FIX: The mock data has 3 valid transactions for this query (t1, t4, t5)
        assert len(transactions) == 3
        transaction_ids = {tx['transaction_id'] for tx in transactions}
        assert 't1' in transaction_ids and 't4' in transaction_ids and 't5' in transaction_ids

    def test_query_2_product_metrics(self, producer_and_listener):
        producer, _, result_queue = producer_and_listener
        QUERY_ID = 2
        
        # 1. Create joined data
        joined_df = pd.merge(self.mock_data['transaction_items'], self.mock_data['menu_items'], on='item_id')
        joined_data = joined_df.to_dict('records')

        # 2. Send the main batch with joined data
        main_batch = create_data_batch(QUERY_ID, NewTransactionItemsMenuItems, joined_data, 1, is_eof=True)
        producer.send(main_batch)

        # 3. Send accounting EOFs for source tables
        producer.send(create_data_batch(QUERY_ID, NewTransactionItems, [], 1, is_eof=True))
        producer.send(create_data_batch(QUERY_ID, NewMenuItems, [], 1, is_eof=True))
        
        result = result_queue.get(timeout=10)

        assert result['status'] == 'success'
        res_data = result['result']
        assert res_data['2024-01']['by_revenue'][0]['name'] == 'Espresso'
        assert res_data['2024-02']['by_quantity'][0]['name'] == 'Latte'
        
    def test_query_3_tpv_analysis_with_joined_data(self, producer_and_listener):
        producer, _, result_queue = producer_and_listener
        QUERY_ID = 3
        
        # 1. Create joined data and split into two shards
        joined_df = pd.merge(self.mock_data['transactions'], self.mock_data['stores'], on='store_id')
        joined_data = joined_df.to_dict('records')
        shard1_data = joined_data[:len(joined_data)//2]
        shard2_data = joined_data[len(joined_data)//2:]

        # 2. Send the two shards for the main joined data batch
        # Note: Both are batch_num=1, but different shards. EOF is on the last shard.
        producer.send(create_data_batch(QUERY_ID, NewTransactionStores, shard1_data, 1, is_eof=False, shard_num=1, total_shards=2))
        producer.send(create_data_batch(QUERY_ID, NewTransactionStores, shard2_data, 1, is_eof=True, shard_num=2, total_shards=2))
        
        # 3. Send accounting EOFs for source tables (these are not sharded)
        producer.send(create_data_batch(QUERY_ID, NewTransactions, [], 1, is_eof=True))
        producer.send(create_data_batch(QUERY_ID, NewStores, [], 1, is_eof=True))

        result = result_queue.get(timeout=15)
        
        assert result['status'] == 'success'
        res_data = result['result']
        assert res_data['Downtown']['2024-S1'] == 180.0
        assert res_data['Uptown']['2024-S2'] == 200.0

    def test_query_4_top_customers_with_joined_data(self, producer_and_listener):
        producer, _, result_queue = producer_and_listener
        QUERY_ID = 4

        # 1. Create joined data
        tx_stores = pd.merge(self.mock_data['transactions'], self.mock_data['stores'], on='store_id')
        joined_df = pd.merge(tx_stores, self.mock_data['users'], on='user_id')
        joined_data = joined_df.to_dict('records')

        # 2. Send the main batch with joined data
        main_batch = create_data_batch(QUERY_ID, NewTransactionStoresUsers, joined_data, 1, is_eof=True)
        producer.send(main_batch)

        # 3. Send accounting EOFs for source tables
        producer.send(create_data_batch(QUERY_ID, NewTransactions, [], 1, is_eof=True))
        producer.send(create_data_batch(QUERY_ID, NewStores, [], 1, is_eof=True))
        producer.send(create_data_batch(QUERY_ID, NewUsers, [], 1, is_eof=True))

        result = result_queue.get(timeout=10)

        assert result['status'] == 'success'
        res_data = result['result']
        
        top_customers_store1 = res_data['Downtown']
        assert len(top_customers_store1) == 2
        assert top_customers_store1[0]['purchase_count'] == 2
        assert top_customers_store1[0]['birthdate'] == '1985-11-20' # User 102