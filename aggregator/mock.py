from enum import Enum
import logging
from aggregator.processing import process_query_2, process_query_3, process_query_4_transactions, process_query_4_users
from aggregator.queryid import QueryId
from protocol.constants import Opcodes
from protocol.databatch import DataBatch
from protocol.entities import RawMenuItems, RawStore, RawTransaction, RawTransactionItem, RawUser
from protocol.messages import EOFMessage, NewMenuItems, NewStores, NewTransactionItems, NewTransactions, NewUsers

class QueryId(Enum):
    FIRST_QUERY = 1
    SECOND_QUERY = 2
    THIRD_QUERY = 3
    FOURTH_QUERY = 4

def create_mock_transactions_databatch(id) -> DataBatch:
    """Create a mock DataBatch with real transaction data for testing."""
    try:
        # Datos para query 3 y 4 - más transacciones para diferentes usuarios en años 2024-2025
        mock_data = [
            ("2ae6d188-76c2-4095-b861-ab97d3cd9312", "1", "1", "40.0", "2.0", "38.0", "2024-03-15 08:00:00"),
            ("7d0a474d-62f4-442a-96b6-a5df2bda8832", "1", "1", "35.0", "2.0", "33.0", "2024-04-16 14:30:00"),
            ("85f86fef-fddb-4eef-9dc3-1444553e6108", "1", "2", "30.0", "3.0", "27.0", "2024-08-10 10:00:00"),
            ("4c41d179-f809-4d5a-a5d7-acb25ae1fe98", "1", "2", "50.0", "4.5", "45.5", "2024-08-11 16:21:00"),
            ("51e44c8e-4812-4a15-a9f9-9a46b62424d6", "1", "3", "29.0", "2.0", "27.0", "2025-01-05 12:33:00"),
            ("d449cf8f-e6d5-4b09-a02e-693c7889dee8", "2", "1", "47.0", "2.0", "45.0", "2025-01-06 18:44:00"),
            ("6b00c575-ec6e-4070-82d2-26d66b017b8b", "2", "1", "80.0", "3.0", "77.0", "2025-07-15 09:57:00"),
            ("54fa4304-5131-4382-a8dc-f30cb18155b7", "2", "1", "50.0", "3.0", "47.0", "2025-07-16 21:01:00"),
            ("a1b2c3d4-e5f6-7890-abcd-ef1234567890", "2", "2", "60.0", "5.0", "55.0", "2024-06-20 11:15:00"),
            ("b2c3d4e5-f6g7-8901-bcde-f23456789012", "2", "3", "25.0", "1.0", "24.0", "2024-12-10 16:45:00"),
            ("c3d4e5f6-g7h8-9012-cdef-345678901234", "3", "4", "90.0", "8.0", "82.0", "2024-05-12 13:20:00"),
            ("d4e5f6g7-h8i9-0123-defg-456789012345", "3", "4", "45.0", "3.0", "42.0", "2025-03-08 10:30:00"),
            ("e5f6g7h8-i9j0-1234-efgh-567890123456", "3", "5", "70.0", "7.0", "63.0", "2024-11-25 15:00:00")
        ]
        
        mock_transactions = []
        for transaction_id, store_id, user_id, original_amount, discount_applied, final_amount, created_at in mock_data:
            transaction = RawTransaction(
                transaction_id=transaction_id,
                store_id=store_id,
                payment_method_id="",  # Campo filtrado
                user_id=user_id,
                original_amount=original_amount,
                discount_applied=discount_applied,
                final_amount=final_amount,
                created_at=created_at
            )
            mock_transactions.append(transaction)
        
        # Crear mensaje NewTransactions
        transactions_msg = NewTransactions()
        transactions_msg.rows = mock_transactions
        
        # Crear DataBatch
        databatch = DataBatch()
        databatch.table_ids = [Opcodes.NEW_TRANSACTION]
        databatch.query_ids = [QueryId.FIRST_QUERY.value]
        databatch.shard_num = int(id)
        databatch.total_shards = 1
        databatch.batch_number = 1
        databatch.batch_msg = transactions_msg
        
        logging.debug(f"Created mock DataBatch with {len(mock_transactions)} transactions")
        return databatch
        
    except Exception as e:
        logging.error(f"Error creating mock DataBatch: {e}")
        return DataBatch()

def create_mock_transaction_items_databatch(id) -> DataBatch:
    """Create a mock DataBatch with real transaction items data for testing."""
    try:
        # Datos reales de transaction items - conservando campos filtrados
        mock_data = [
            ("2ae6d188-76c2-4095-b861-ab97d3cd9312", "6", "3", "9.5", "28.5", "2024-07-01 07:00:00"),
            ("2ae6d188-76c2-4095-b861-ab97d3cd9312", "6", "1", "9.5", "9.5", "2024-07-01 07:00:00"),
            ("7d0a474d-62f4-442a-96b6-a5df2bda8832", "5", "3", "9.0", "27.0", "2024-07-01 07:00:02"),
            ("7d0a474d-62f4-442a-96b6-a5df2bda8832", "1", "1", "6.0", "6.0", "2024-07-01 07:00:02"),
            ("85f86fef-fddb-4eef-9dc3-1444553e6108", "7", "3", "9.0", "27.0", "2024-07-01 07:00:04"),
            ("4c41d179-f809-4d5a-a5d7-acb25ae1fe98", "1", "3", "6.0", "18.0", "2024-07-01 07:00:21"),
            ("4c41d179-f809-4d5a-a5d7-acb25ae1fe98", "6", "1", "9.5", "9.5", "2024-07-01 07:00:21"),
            ("4c41d179-f809-4d5a-a5d7-acb25ae1fe98", "7", "2", "9.0", "18.0", "2024-07-01 07:00:21")
        ]
        
        mock_transaction_items = []
        for transaction_id, item_id, quantity, unit_price, subtotal, created_at in mock_data:
            transaction_item = RawTransactionItem(
                transaction_id=transaction_id,
                item_id=item_id,
                quantity=quantity,
                unit_price=unit_price,  # Este campo se desecha en el filtro
                subtotal=subtotal,
                created_at=created_at
            )
            mock_transaction_items.append(transaction_item)
        
        # Crear mensaje NewTransactionItems
        transaction_items_msg = NewTransactionItems()
        transaction_items_msg.rows = mock_transaction_items
        
        # Crear DataBatch
        databatch = DataBatch()
        databatch.table_ids = [Opcodes.NEW_TRANSACTION_ITEMS]
        databatch.query_ids = [QueryId.FIRST_QUERY.value]
        databatch.shard_num = int(id)
        databatch.total_shards = 1
        databatch.batch_number = 1
        databatch.batch_msg = transaction_items_msg
        
        logging.debug(f"Created mock DataBatch with {len(mock_transaction_items)} transaction items")
        return databatch
        
    except Exception as e:
        logging.error(f"Error creating mock DataBatch: {e}")
        return DataBatch()
    
def create_mock_users_databatch(id) -> DataBatch:
    """Create a mock DataBatch with real users data for testing."""
    try:
        # Datos reales de usuarios - conservando solo user_id y birthdate (campos filtrados)
        mock_users = [
            # user_id, gender, birthdate, registered_at
            ("1", "female", "1970-04-22", "2023-01-15 10:00:00"),
            ("2", "male", "1991-12-08", "2023-03-12 11:30:00"),
            ("3", "female", "1971-03-15", "2023-05-17 14:20:00"),
            ("4", "male", "1975-11-08", "2023-07-18 09:45:00"),
            ("5", "male", "2005-05-10", "2023-09-19 16:15:00"),
            ("6", "female", "1966-12-30", "2023-11-20 12:00:00"),
            ("7", "male", "1980-08-22", "2024-01-05 13:10:00"),
            ("8", "female", "1995-02-14", "2024-02-11 09:50:00"),
            ("9", "male", "1985-06-30", "2024-03-23 08:40:00"),
            ("10", "female", "1990-09-05", "2024-04-16 15:25:00")
        ]

        
        mock_user_list = []
        for user_id, gender, birthdate, registered_at in mock_users:
            user = RawUser(
                user_id=user_id,
                gender=gender,        # Este campo se desecha en el filtro
                birthdate=birthdate,
                registered_at=registered_at  # Este campo se desecha en el filtro
            )
            mock_user_list.append(user)
        
        # Crear mensaje NewUsers
        users_msg = NewUsers()
        users_msg.rows = mock_user_list

        # Crear DataBatch
        databatch = DataBatch()
        databatch.table_ids = [Opcodes.NEW_USERS]
        databatch.query_ids = [QueryId.FIRST_QUERY.value]
        databatch.shard_num = int(id)
        databatch.total_shards = 1
        databatch.batch_number = 1
        databatch.batch_msg = users_msg
        
        logging.debug(f"Created mock DataBatch with {len(mock_user_list)} users")
        return databatch
        
    except Exception as e:
        logging.error(f"Error creating mock DataBatch: {e}")
        return DataBatch()

def create_mock_menu_items_databatch(id) -> DataBatch:
    """Create a mock DataBatch with real menu items data for testing."""
    try:
        # Datos reales de menu items - conservando product_id, name, price (campos filtrados)
        mock_data = [
            ("1", "Espresso", "coffee", "6.0", "False", "", ""),
            ("2", "Americano", "coffee", "7.0", "False", "", ""),
            ("3", "Latte", "coffee", "8.0", "False", "", ""),
            ("4", "Cappuccino", "coffee", "8.0", "False", "", ""),
            ("5", "Flat White", "coffee", "9.0", "False", "", ""),
            ("6", "Mocha", "coffee", "9.5", "False", "", ""),
            ("7", "Hot Chocolate", "non-coffee", "9.0", "False", "", ""),
            ("8", "Matcha Latte", "non-coffee", "10.0", "False", "", "")
        ]
        
        mock_menu_items = []
        for product_id, name, category, price, is_seasonal, available_from, available_to in mock_data:
            menu_item = RawMenuItems(
                product_id=product_id,
                name=name,
                price=price,
                category=category,        # Este campo se desecha en el filtro
                is_seasonal=is_seasonal,  # Este campo se desecha en el filtro
                available_from=available_from,  # Este campo se desecha en el filtro
                available_to=available_to       # Este campo se desecha en el filtro
            )
            mock_menu_items.append(menu_item)
        
        # Crear mensaje NewMenuItems
        menu_items_msg = NewMenuItems()
        menu_items_msg.rows = mock_menu_items

        # Crear DataBatch
        databatch = DataBatch()
        databatch.table_ids = [Opcodes.NEW_MENU_ITEMS]
        databatch.query_ids = [QueryId.FIRST_QUERY.value]
        databatch.shard_num = int(id)
        databatch.total_shards = 1
        databatch.batch_number = 1
        databatch.batch_msg = menu_items_msg
        
        logging.debug(f"Created mock DataBatch with {len(mock_menu_items)} menu items")
        return databatch
        
    except Exception as e:
        logging.error(f"Error creating mock DataBatch: {e}")
        return DataBatch()

def create_mock_stores_databatch(id) -> DataBatch:
    """Create a mock DataBatch with real stores data for testing."""
    try:
        # Datos reales de stores - conservando store_id y store_name (campos filtrados)
        mock_data = [
            ("1", "G Coffee @ USJ 89q", "Jalan Dewan Bahasa 5/9", "50998", "USJ 89q", "Kuala Lumpur", "3.117134", "101.615027"),
            ("2", "G Coffee @ Kondominium Putra", "Jln Yew 6X", "63826", "Kondominium Putra", "Selangor Darul Ehsan", "2.959571", "101.51772"),
            ("3", "G Coffee @ USJ 57W", "Jalan Bukit Petaling 5/16C", "62094", "USJ 57W", "Putrajaya", "2.951038", "101.663698"),
            ("4", "G Coffee @ Kampung Changkat", "Jln 6/6A", "62941", "Kampung Changkat", "Putrajaya", "2.914594", "101.704486"),
            ("5", "G Coffee @ Seksyen 21", "Jalan Anson 4k", "62595", "Seksyen 21", "Putrajaya", "2.937599", "101.698478"),
            ("6", "G Coffee @ Alam Tun Hussein Onn", "Jln Pasar Besar 63s", "63518", "Alam Tun Hussein Onn", "Selangor Darul Ehsan", "3.279175", "101.784923"),
            ("7", "G Coffee @ Damansara Saujana", "Jln 8/74", "65438", "Damansara Saujana", "Selangor Darul Ehsan", "3.22081", "101.58459"),
            ("8", "G Coffee @ Bandar Seri Mulia", "Jalan Wisma Putra", "58621", "Bandar Seri Mulia", "Kuala Lumpur", "3.140674", "101.706562"),
            ("9", "G Coffee @ PJS8", "Jalan 7/3o", "62418", "PJS8", "Putrajaya", "2.952444", "101.702623"),
            ("10", "G Coffee @ Taman Damansara", "Jln 2", "67102", "Taman Damansara", "Selangor Darul Ehsan", "3.497178", "101.595271")
        ]
        
        mock_stores = []
        for store_id, store_name, street, postal_code, city, state, latitude, longitude in mock_data:
            store = RawStore(
                store_id=store_id,
                store_name=store_name,
                street=street,           # Este campo se desecha en el filtro
                postal_code=postal_code, # Este campo se desecha en el filtro
                city=city,               # Este campo se desecha en el filtro
                state=state,             # Este campo se desecha en el filtro
                latitude=latitude,       # Este campo se desecha en el filtro
                longitude=longitude      # Este campo se desecha en el filtro
            )
            mock_stores.append(store)
        
        # Crear mensaje NewStores
        stores_msg = NewStores()
        stores_msg.rows = mock_stores

        # Crear DataBatch
        databatch = DataBatch()
        databatch.table_ids = [Opcodes.NEW_STORES]
        databatch.query_ids = [QueryId.FIRST_QUERY.value]
        databatch.shard_num = int(id)
        databatch.total_shards = 1
        databatch.batch_number = 1
        databatch.batch_msg = stores_msg
        
        logging.debug(f"Created mock DataBatch with {len(mock_stores)} stores")
        return databatch
        
    except Exception as e:
        logging.error(f"Error creating mock DataBatch: {e}")
        return DataBatch()

def create_mock_eof_databatch(id, table_type: str = "transactions") -> DataBatch:
    """Create a mock EOF DataBatch for testing."""
    try:
        # Crear mensaje EOF
        eof_msg = EOFMessage()
        eof_msg.create_eof_message(
            batch_number=1, 
            table_type=table_type
        )
        
        # Crear DataBatch
        databatch = DataBatch()
        databatch.table_ids = [Opcodes.EOF]
        databatch.query_ids = [QueryId.FIRST_QUERY.value]
        databatch.shard_num = int(id)
        databatch.total_shards = 1
        databatch.batch_number = 1
        databatch.batch_msg = eof_msg
        
        logging.debug(f"Created mock EOF DataBatch for table_type: {table_type}")
        return databatch
        
    except Exception as e:
        logging.error(f"Error creating mock EOF DataBatch: {e}")
        return DataBatch()

def obtain_table_message(databatch: DataBatch):
    """Obtain the table message from a DataBatch and log its contents."""
    table_message = databatch.batch_msg
    return table_message.rows

def mock_process(id):
    logging.debug('Processing query 2')
    transactions_items_databatch = create_mock_transaction_items_databatch(id)
    transaction_items = obtain_table_message(transactions_items_databatch)
    process_query_2(transaction_items)
    
    logging.debug('Processing query 3')
    transactions_databatch = create_mock_transactions_databatch(id)
    transactions = obtain_table_message(transactions_databatch)
    process_query_3(transactions)

    # Query 4 parte 1: transactions
    transactions_databatch = create_mock_transactions_databatch(id)
    transactions = obtain_table_message(transactions_databatch)
    process_query_4_transactions(transactions)
    
    # Query 4 parte 1: users
    users_databatch = create_mock_users_databatch(id)
    users = obtain_table_message(users_databatch)
    process_query_4_users(users)