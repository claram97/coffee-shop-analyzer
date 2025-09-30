#!/usr/bin/env python3
"""
Coffee Shop Data Aggregator

This module implements the main aggregator functionality for collecting,
processing and analyzing coffee shop data from multiple clients.
"""
from enum import Enum
import logging
from collections import defaultdict
from datetime import datetime
# from middleware.middleware_client import MessageMiddlewareExchange, MessageMiddlewareQueue
from protocol.databatch import DataBatch
from protocol.constants import Opcodes
from protocol.messages import NewTransactionItems, NewTransactions, NewUsers, NewMenuItems, NewStores
from protocol.entities import RawTransaction, RawMenuItems, RawStore, RawTransactionItem, RawUser

# Constants
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
VALID_YEARS = [2024, 2025]
MIN_HOUR = 6  # 06:00
MAX_HOUR = 23  # 23:00
FIRST_SEMESTER_MONTHS = 6  # Months 1-6 = Semester 1
SEMESTER_1 = 1
SEMESTER_2 = 2

class QueryId(Enum):
    FIRST_QUERY = 1
    SECOND_QUERY = 2
    THIRD_QUERY = 3
    FOURTH_QUERY = 4


# Para transactions y query 1: re-enviar
# Para transaction_items y query 2:
# Para transactions y query 3:
# Para transactions y query 4:

class Aggregator:
    """Main aggregator class for coffee shop data analysis."""
    
    def __init__(self, id: str):
        """
        Initialize the aggregator.
        
        Args:
          id (int): The unique identifier for the aggregator instance.
        """
        self.id = id
        self.running = False
        
    #     # Input exchanges for different data types
    #     if self.id == "00":
    #         self._menu_items_exchange = MessageMiddlewareExchange(
    #             host="localhost",
    #             exchange_name="ex.menu_items",
    #             route_keys=[f"tx.shard.{self.id}"])
    #         self._stores_exchange = MessageMiddlewareExchange(
    #             host="localhost",
    #             exchange_name="ex.stores", 
    #             route_keys=[f"tx.shard.{self.id}"])
    #     self._transactions_exchange = MessageMiddlewareExchange(
    #         host="localhost",
    #         exchange_name="ex.transactions", 
    #         route_keys=[f"tx.shard.{self.id}"])
    #     self._transaction_items_exchange = MessageMiddlewareExchange(
    #         host="localhost",
    #         exchange_name="ex.transaction_items", 
    #         route_keys=[f"tx.shard.{self.id}"])
    #     self._users_exchange = MessageMiddlewareExchange(
    #         host="localhost",
    #         exchange_name="ex.users", 
    #         route_keys=[f"tx.shard.{self.id}"])

    #     self.joiner_queue = MessageMiddlewareQueue(
    #         host="localhost",
    #         queue_name="aggregator_to_joiner_queue"
    #     )
    
    # def run(self):
    #     """Start the aggregator server."""
    #     self.running = True
    #     self._menu_items_exchange.start_consuming(self._handle_menu_item)
    #     self._stores_exchange.start_consuming(self._handle_store)
    #     self._transactions_exchange.start_consuming(self._handle_transaction)
    #     self._transaction_items_exchange.start_consuming(self._handle_transaction_item)
    #     self._users_exchange.start_consuming(self._handle_user)
    #     logging.debug("Started aggregator server")
    
    # def stop(self):
    #     """Stop the aggregator server."""
    #     self.running = False
    #     logging.debug("Stopping aggregator server")
    
    # def _handle_menu_item(self, message: bytes) -> bool:
    #     """Process incoming menu item messages."""
    #     try:
    #         logging.info(f"Received menu item.  Passing to joiner.")
    #         self.joiner_queue.send(message)
    #         return True
    #     except:
    #         logging.error(f"Failed to decode menu item message")
    #         return False
    
    # def _handle_store(self, message: bytes) -> bool:
    #     """Process incoming store messages."""
    #     try:
    #         logging.info(f"Received store. Passing to joiner.")
    #         self.joiner_queue.send(message)
    #         return True
    #     except:
    #         logging.error(f"Failed to decode store message")
    #         return False
    
    # def _handle_transaction(self, message: bytes) -> bool:
    #     """Process incoming transaction messages."""
    #     try:
    #         logging.info(f"Received transaction")
    #         transaction = DataBatch.deserialize_from_bytes(message)
    #         # Esta lista tiene una sola posición, así que tomamos el primer elemento
    #         query_id = transaction.query_ids[0]
    #         table_id = transaction.table_ids[0]
    #         logging.info(f"Transaction belongs to table {table_id} and query {query_id}")
    #         if query_id == QueryId.FIRST_QUERY:
    #             logging.info("First query transaction received.")
    #         elif query_id == QueryId.SECOND_QUERY:
    #             logging.info("Second query transaction received.")
    #         elif query_id == QueryId.THIRD_QUERY:
    #             logging.info("Third query transaction received.")
    #         elif query_id == QueryId.FOURTH_QUERY:
    #             logging.info("Fourth query transaction received.")
            
    #         self.joiner_queue.send(message)
    #         return True
    #     except:
    #         logging.error(f"Failed to decode transaction message")
    #         return False
    
    # def _handle_transaction_item(self, message: bytes) -> bool:
    #     """Process incoming transaction item messages."""
    #     try:
    #         logging.info(f"Received transaction item")
    #         transaction_items_databatch = DataBatch.deserialize_from_bytes(message)
    #         # transaction_items_databatch.
    #         self.joiner_queue.send(message)
    #         return True
    #     except:
    #         logging.error(f"Failed to decode transaction item message")
    #         return False
    
    # def _handle_user(self, message: bytes) -> bool:
    #     """Process incoming user messages."""
    #     try:
    #         logging.info(f"Received user")
    #         user = DataBatch.deserialize_from_bytes(message)
            
    #         self.joiner_queue.send(message)
    #         return True
    #     except:
    #         logging.error(f"Failed to decode user message")
    #         return False
    
    def run(self):
        """Start the aggregator server."""
        self.running = True
        logging.debug("Started aggregator server")

        # self.process_query_1([])
        # self.process_query_2([])
        # self.process_query_3([])
        # self.process_query_4([], [])
        
        transactions_items_databatch = self.create_mock_transaction_items_databatch()
        logging.info(transactions_items_databatch.query_ids)
        logging.info(transactions_items_databatch.batch_bytes)
        logging.info(transactions_items_databatch.batch_msg)
        logging.info(transactions_items_databatch.batch_number)
        logging.info(transactions_items_databatch.meta)
        logging.info(transactions_items_databatch.opcode)
        logging.info(transactions_items_databatch.reserved_u16)
        logging.info(transactions_items_databatch.total_shards)
        logging.info(transactions_items_databatch.table_ids)
        logging.info(transactions_items_databatch.shard_num)
        logging.info(transactions_items_databatch.total_shards)
        
        self.running = False
    
    # def process_query_1(self, transactions: list[RawTransaction]):
    #     logging.debug('Processing query 1')
    #     transactions_items_databatch = self.create_mock_transaction_items_databatch()
    #     transaction_items = self.obtain_table_message(transactions_items_databatch)
    #     self.log_transaction_items(transaction_items)
    #     logging.debug('')
    
    def process_query_2(self, transaction_items: list[RawTransactionItem]):
        logging.debug('Processing query 2')
        transactions_items_databatch = self.create_mock_transaction_items_databatch()
        transaction_items = self.obtain_table_message(transactions_items_databatch)
        
        # Agregar year(created_at), month(created_at), item_id con agregaciones
        from collections import defaultdict
        from datetime import datetime
        
        # Diccionario para agrupar: (year, month, item_id) -> {total_quantity, total_revenue}
        aggregated_data = defaultdict(lambda: {'total_quantity': 0.0, 'total_revenue': 0.0})
        
        for item in transaction_items:
            # Parsear fecha para extraer año y mes
            created_at = datetime.strptime(item.created_at, DATETIME_FORMAT)
            year = created_at.year
            month = created_at.month
            item_id = item.item_id
            
            # Crear clave de agrupación
            key = (year, month, item_id)
            
            # Sumar quantity y subtotal
            aggregated_data[key]['total_quantity'] += float(item.quantity)
            aggregated_data[key]['total_revenue'] += float(item.subtotal)
        
        # Log de los resultados agregados
        logging.debug('Query 2 Results - Aggregated by year, month, item_id:')
        logging.debug('Year | Month | Item_ID | Total_Quantity | Total_Revenue')
        logging.debug('-' * 60)
        
        for (year, month, item_id), totals in aggregated_data.items():
            logging.debug(f'{year:4d} | {month:5d} | {item_id:7s} | {totals["total_quantity"]:13.1f} | {totals["total_revenue"]:12.2f}')
        
        logging.debug(f'Total groups: {len(aggregated_data)}')
        logging.debug('')
    
    def process_query_3(self, transactions: list[RawTransaction]):
        logging.debug('Processing query 3')
        transactions_databatch = self.create_mock_transactions_databatch()
        transactions = self.obtain_table_message(transactions_databatch)
        
        # Query 3: Agregar por (store_id, year, semester) con condiciones específicas
        aggregated_data = defaultdict(lambda: {
            'transaction_count': 0,
            'total_original_amount': 0.0,
            'total_discount_applied': 0.0,
            'total_final_amount': 0.0
        })
        
        filtered_count = 0
        total_count = len(transactions)
        
        for transaction in transactions:
            # Parsear fecha y hora
            created_at = datetime.strptime(transaction.created_at, DATETIME_FORMAT)
            year = created_at.year
            month = created_at.month
            hour = created_at.hour
            
            # Aplicar condiciones:
            # 1. Año ∈ {2024, 2025}
            if year not in VALID_YEARS:
                continue
                
            # 2. Hora entre 06:00 y 23:00
            if hour < MIN_HOUR or hour > MAX_HOUR:
                continue
            
            filtered_count += 1
            
            # Calcular semestre: 1-6 = S1, 7-12 = S2
            semester = SEMESTER_1 if month <= FIRST_SEMESTER_MONTHS else SEMESTER_2
            
            # Crear clave de agrupación
            key = (transaction.store_id, year, semester)
            
            # Agregar métricas
            aggregated_data[key]['transaction_count'] += 1
            aggregated_data[key]['total_original_amount'] += float(transaction.original_amount)
            aggregated_data[key]['total_discount_applied'] += float(transaction.discount_applied)
            aggregated_data[key]['total_final_amount'] += float(transaction.final_amount)
        
        # Log de los resultados agregados
        logging.debug('Query 3 Results - Aggregated by store_id, year, semester:')
        valid_years_str = f"{{{VALID_YEARS[0]},{VALID_YEARS[1]}}}"
        logging.debug(f'Filtered {filtered_count}/{total_count} transactions (year ∈ {valid_years_str}, hour ∈ [{MIN_HOUR:02d}:00-{MAX_HOUR:02d}:00])')
        logging.debug('Store_ID | Year | Semester | Count | Original_Amount | Discounts | Final_Amount')
        logging.debug('-' * 80)
        
        for (store_id, year, semester), totals in aggregated_data.items():
            logging.debug(f'{store_id:8s} | {year:4d} | {semester:8d} | {totals["transaction_count"]:5d} | '
                        f'{totals["total_original_amount"]:14.2f} | {totals["total_discount_applied"]:9.2f} | '
                        f'{totals["total_final_amount"]:11.2f}')
        
        logging.debug(f'Total groups: {len(aggregated_data)}')
        logging.debug('')
        

    def process_query_4(self, transactions: list[RawTransaction], users: list[RawUser]):
        logging.debug('Processing query 4')
        
        # TABLA 1: Procesar transacciones
        transactions_databatch = self.create_mock_transactions_databatch()
        transactions = self.obtain_table_message(transactions_databatch)
        
        # Filtrar transacciones por año 2024-2025 y agrupar por (store_id, user_id)
        transaction_counts = defaultdict(int)
        filtered_count = 0
        total_count = len(transactions)
        
        for transaction in transactions:
            # Parsear fecha
            created_at = datetime.strptime(transaction.created_at, DATETIME_FORMAT)
            year = created_at.year
            
            # Filtrar por años válidos
            if year not in VALID_YEARS:
                continue
                
            filtered_count += 1
            
            # Agrupar por (store_id, user_id) y contar transacciones
            key = (transaction.store_id, transaction.user_id)
            transaction_counts[key] += 1
        
        # TABLA 2: Procesar usuarios (sin filtros, solo datos base)
        users_databatch = self.create_mock_users_databatch()
        users = self.obtain_table_message(users_databatch)
        
        # Log de resultados - TABLA 1: Transacciones agregadas
        logging.debug(f'Query 4 - TABLE 1: Transaction counts by store and user ({VALID_YEARS[0]}-{VALID_YEARS[1]}):')
        logging.debug(f'Filtered {filtered_count}/{total_count} transactions (year ∈ {{{VALID_YEARS[0]},{VALID_YEARS[1]}}})')
        logging.debug('Store_ID | User_ID | Total_Purchases')
        logging.debug('-' * 40)
        
        for (store_id, user_id), total_purchases in transaction_counts.items():
            logging.debug(f'{store_id:8s} | {user_id:7s} | {total_purchases:15d}')
        
        logging.debug(f'Total customer-store combinations: {len(transaction_counts)}')
        logging.debug('')
        
        # Log de resultados - TABLA 2: Usuarios
        logging.debug('Query 4 - TABLE 2: Users data:')
        logging.debug('User_ID | Birthdate')
        logging.debug('-' * 25)
        
        for user in users:
            logging.debug(f'{user.user_id:7s} | {user.birthdate}')
        
        logging.debug(f'Total users: {len(users)}')
        logging.debug('')
     
    def obtain_table_message(self, databatch: DataBatch):
        """Obtain the table message from a DataBatch and log its contents."""
        table_message = databatch.batch_msg
        return table_message.rows
    
    def log_transaction_items(self, table_message):
        for item in table_message:
            logging.info(f"Transaction Item: {item.transaction_id}, {item.item_id}, {item.quantity}, {item.subtotal}, {item.created_at}")
    
    def log_transactions(self, table_message):
        for item in table_message:
            logging.info(f"Transaction: {item.transaction_id}, {item.store_id}, {item.user_id}, {item.final_amount}, {item.created_at}")
    
    def log_users(self, table_message):
        for item in table_message:
            logging.info(f"User: {item.user_id}, {item.birthdate}")    
    
    def create_mock_transactions_databatch(self) -> DataBatch:
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
            databatch.shard_num = int(self.id)
            databatch.total_shards = 1
            databatch.batch_number = 1
            databatch.batch_msg = transactions_msg
            
            logging.debug(f"Created mock DataBatch with {len(mock_transactions)} transactions")
            return databatch
            
        except Exception as e:
            logging.error(f"Error creating mock DataBatch: {e}")
            return DataBatch()
    
    def create_mock_transaction_items_databatch(self) -> DataBatch:
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
            databatch.shard_num = int(self.id)
            databatch.total_shards = 1
            databatch.batch_number = 1
            databatch.batch_msg = transaction_items_msg
            
            logging.debug(f"Created mock DataBatch with {len(mock_transaction_items)} transaction items")
            return databatch
            
        except Exception as e:
            logging.error(f"Error creating mock DataBatch: {e}")
            return DataBatch()
        
    def create_mock_users_databatch(self) -> DataBatch:
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
            databatch.shard_num = int(self.id)
            databatch.total_shards = 1
            databatch.batch_number = 1
            databatch.batch_msg = users_msg
            
            logging.debug(f"Created mock DataBatch with {len(mock_user_list)} users")
            return databatch
            
        except Exception as e:
            logging.error(f"Error creating mock DataBatch: {e}")
            return DataBatch()

    def create_mock_menu_items_databatch(self) -> DataBatch:
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
            databatch.shard_num = int(self.id)
            databatch.total_shards = 1
            databatch.batch_number = 1
            databatch.batch_msg = menu_items_msg
            
            logging.debug(f"Created mock DataBatch with {len(mock_menu_items)} menu items")
            return databatch
            
        except Exception as e:
            logging.error(f"Error creating mock DataBatch: {e}")
            return DataBatch()

    def create_mock_stores_databatch(self) -> DataBatch:
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
            databatch.shard_num = int(self.id)
            databatch.total_shards = 1
            databatch.batch_number = 1
            databatch.batch_msg = stores_msg
            
            logging.debug(f"Created mock DataBatch with {len(mock_stores)} stores")
            return databatch
            
        except Exception as e:
            logging.error(f"Error creating mock DataBatch: {e}")
            return DataBatch()