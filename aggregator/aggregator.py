#!/usr/bin/env python3
"""
Coffee Shop Data Aggregator

This module implements the main aggregator functionality for collecting,
processing, and analyzing coffee shop data from multiple clients.

The aggregator acts as an intermediary component in the data processing pipeline,
receiving filtered data batches from filter routers, performing query-specific
processing (for queries 2, 3, and 4), and forwarding the results to joiner routers
for final data joining operations.

Key responsibilities:
    - Receiving data batches from filter routers via message exchanges
    - Processing transaction and transaction_item data for specific queries
    - Routing processed data to appropriate joiner router replicas
    - Managing EOF (End of File) messages and cleanup operations
    - Maintaining client-to-replica mappings for proper data routing
"""
import logging
import os
from collections import defaultdict
from typing import Dict, Optional, Tuple

from processing import (
    process_query_2,
    process_query_3,
    process_query_4_transactions,
    serialize_query2_results,
    serialize_query3_results,
    serialize_query4_transaction_results,
)

from app_config.config_loader import Config
from middleware.middleware_client import (
    MessageMiddlewareExchange,
    MessageMiddlewareQueue,
)
from protocol2.databatch_pb2 import DataBatch
from protocol2.envelope_pb2 import Envelope, MessageType
from protocol2.table_data_pb2 import TableName
from protocol2.table_data_utils import build_table_data, iterate_rows_as_dicts

TID_TO_NAME = {
    TableName.MENU_ITEMS: "menu_items",
    TableName.STORES: "stores",
    TableName.TRANSACTION_ITEMS: "transaction_items",
    TableName.TRANSACTIONS: "transactions",
    TableName.USERS: "users",
}
LIGHT_TABLES = {"menu_items", "stores"}


class ExchangePublisherPool:
    """Pool for managing and reusing message exchange connections.

    This class maintains a pool of MessageMiddlewareExchange instances to avoid
    creating duplicate connections for the same exchange configuration. Exchanges
    are keyed by (exchange_name, route_keys, queue_name) tuple.

    Attributes:
        _host: The message broker host address.
        _pool: Dictionary mapping exchange configurations to exchange instances.
    """

    def __init__(self, host):
        """
        Initialize the exchange publisher pool.

        Args:
            host (str): The message broker host address.
        """
        self._host = host
        self._pool = {}
        logging.info("Created exchange publisher pool for host: %s", host)

    def get_exchange(self, exchange_name, route_keys, queue_name=None):
        """
        Get or create a message exchange connection.

        If an exchange with the same configuration already exists in the pool,
        it is reused. Otherwise, a new exchange is created and added to the pool.

        Args:
            exchange_name (str): Name of the message exchange.
            route_keys (str or list[str]): Routing key(s) for the exchange.
            queue_name (str, optional): Name of the queue to bind to the exchange.

        Returns:
            MessageMiddlewareExchange: The exchange instance for the given
                configuration.
        """
        key = (
            exchange_name,
            tuple(route_keys) if isinstance(route_keys, list) else (route_keys,),
            queue_name,
        )
        if key not in self._pool:
            logging.info(
                "Creating new exchange in pool: %s with keys %s",
                exchange_name,
                route_keys,
            )
            self._pool[key] = MessageMiddlewareExchange(
                host=self._host,
                exchange_name=exchange_name,
                route_keys=route_keys,
                queue_name=queue_name,
            )
        else:
            logging.debug("Reusing existing exchange from pool: %s", exchange_name)
        return self._pool[key]


class Aggregator:
    """Main aggregator class for coffee shop data analysis.

    The Aggregator receives data batches from filter routers, performs
    query-specific processing on transaction and transaction_item data,
    and forwards the results to joiner routers. It handles multiple message
    types including data batches, EOF messages, and cleanup messages.

    The aggregator maintains connections to:
        - Input exchanges from filter routers (one per table)
        - Output queues to joiner routers (one per table/replica combination)

    Attributes:
        id (str): Unique identifier for this aggregator instance.
        running (bool): Flag indicating if the aggregator is currently running.
        config_path (str): Path to the configuration file.
        config (Config): Configuration object loaded from config_path.
        host (str): Message broker host address.
        _jr_replicas (int): Number of joiner router replicas.
        _exchange_pool (ExchangePublisherPool): Pool for managing exchange connections.
        _exchanges (dict): Dictionary mapping table names to input exchanges.
        _out_queues (dict): Dictionary mapping (table, replica) tuples to output queues.
        _client_table_replicas (dict): Tracks which replicas each client/table
            combination has used for proper EOF routing.
    """

    def __init__(self, id: str):
        """
        Initialize the aggregator.

        Sets up message exchanges for receiving data from filter routers and
        output queues for sending data to joiner routers. Configures connections
        for all supported tables: menu_items, stores, transactions,
        transaction_items, and users.

        Args:
            id (str): The unique identifier for the aggregator instance.
        """
        self.id = id
        self.running = False
        self.config_path = os.getenv("CONFIG_PATH", "/config/config.ini")
        self.config = Config(self.config_path)
        self.host = self.config.broker.host
        self._jr_replicas = self.config.routers.joiner

        self._exchange_pool = ExchangePublisherPool(host=self.host)
        logging.info("Initialized exchange pool for aggregator %s", id)

        tables = ["menu_items", "stores", "transactions", "transaction_items", "users"]
        self._exchanges = {}

        for table in tables:
            exchange_name = self.config.filter_router_exchange(table)
            routing_key = self.config.filter_router_rk(table, self.id)
            queue_name = self.config.aggregator_queue(table, self.id)

            self._exchanges[table] = self._exchange_pool.get_exchange(
                exchange_name=exchange_name,
                route_keys=[routing_key],
                queue_name=queue_name,
            )
            logging.info(
                "Created exchange connection for %s: %s -> %s with queue %s",
                table,
                exchange_name,
                routing_key,
                queue_name,
            )

        self._out_queues = {}
        for table in tables:
            for replica in range(0, self._jr_replicas):
                out_q = self.config.aggregator_to_joiner_router_queue(
                    table, self.id, replica
                )
                self._out_queues[(table, replica)] = MessageMiddlewareQueue(
                    host=self.host, queue_name=out_q
                )

        self._client_table_replicas: Dict[Tuple[str, str], set[int]] = defaultdict(set)

    def _target_replicas(
        self, table: str, client_id: Optional[str], batch_number: int
    ) -> list[int]:
        """
        Determine which joiner router replicas should receive data for a given table.

        For light tables (menu_items, stores), data is sent to all replicas.
        For heavy tables (transactions, transaction_items, users), data is
        sharded to a single replica based on batch_number.

        Args:
            table (str): Name of the table.
            client_id (str, optional): Client identifier for tracking replica usage.
            batch_number (int): Batch number used for sharding heavy tables.

        Returns:
            list[int]: List of replica indices that should receive this data.
        """
        if table in LIGHT_TABLES:
            replicas = list(range(self._jr_replicas))
        else:
            shard = batch_number if batch_number is not None else 0
            replicas = [shard % self._jr_replicas]
        if client_id:
            self._client_table_replicas[(table, client_id)].update(replicas)
        return replicas

    def _send_to_joiner_by_table(
        self, table: str, client_id: Optional[str], batch_number: int, raw_bytes: bytes
    ):
        """
        Send raw message bytes to joiner router replicas for a specific table.

        Determines target replicas using _target_replicas and sends the message
        to each replica's output queue.

        Args:
            table (str): Name of the table.
            client_id (str, optional): Client identifier.
            batch_number (int): Batch number for sharding.
            raw_bytes (bytes): Raw serialized message bytes to send.

        Raises:
            Exception: If no output queue is configured for the table/replica,
                or if the aggregator is shutting down.
        """
        if not self.running:
            logging.warning(
                "Aggregator shutting down, refusing to send message for table=%s", table
            )
            raise Exception("Aggregator is shutting down")
        logging.info("Sending to joiner by table=%s", table)
        replicas = self._target_replicas(table, client_id, batch_number)
        for replica in replicas:
            if not self.running:
                logging.warning(
                    "Aggregator shutting down, aborting send to table=%s replica=%s",
                    table,
                    replica,
                )
                raise Exception("Aggregator is shutting down")
            q = self._out_queues.get((table, replica))
            if not q:
                logging.error(
                    "No out queue configured for table=%s replica=%s", table, replica
                )
                raise Exception(f"No out queue configured for table={table}")
            q.send(raw_bytes)

    def _forward_databatch_by_table(
        self, data_batch: DataBatch, raw: bytes, table_name: str
    ):
        """
        Forward a DataBatch message to the appropriate joiner router queue.

        Extracts the batch number from the data batch and routes the message
        to the correct joiner router replica(s) based on the table type and
        sharding strategy.

        Args:
            data_batch (DataBatch): The parsed DataBatch protobuf message.
            raw (bytes): Raw serialized message bytes to forward.
            table_name (str): Name of the table this data batch belongs to.

        Raises:
            Exception: If forwarding fails or table_name is invalid.
        """
        logging.info("Forwarding DataBatch message")
        try:
            table = table_name
            logging.debug("Forwarding DataBatch to table=%s", table)
            if not table:
                logging.error("Unknown table_name=%s", table_name)
                return

            batch_number = int(getattr(data_batch.payload, "batch_number", 0) or 0)
            self._send_to_joiner_by_table(
                table, data_batch.client_id, batch_number, raw
            )
        except Exception:
            logging.exception(
                "Failed to forward databatch by table. Table: %s", table_name
            )
            raise

    def _forward_eof(self, raw: bytes, table_name: str):
        """
        Forward an EOF (End of File) message to joiner router replicas.

        Parses the EOF message and determines which replicas should receive it.
        For light tables or when no client_id is present, sends to all replicas.
        For heavy tables with a client_id, sends only to replicas that received
        data for that client/table combination.

        Args:
            raw (bytes): Raw serialized EOF message bytes.
            table_name (str): Name of the table this EOF belongs to.

        Raises:
            Exception: If EOF forwarding fails, no output queue is configured,
                or aggregator is shutting down.
        """
        if not self.running:
            logging.warning(
                "Aggregator shutting down, refusing to forward EOF for table=%s",
                table_name,
            )
            raise Exception("Aggregator is shutting down")
        logging.debug("Forwarding EOF message")
        try:
            envelope = Envelope()
            envelope.ParseFromString(raw)
            eof = envelope.eof

            original_trace = eof.trace if eof.trace else ""
            logging.info(
                "Forwarding EOF for table=%s with trace=%s", table_name, original_trace
            )
            client_id = eof.client_id if eof.client_id else None
            replicas: list[int]
            if table_name in LIGHT_TABLES or not client_id:
                replicas = list(range(self._jr_replicas))
            else:
                replicas = list(
                    self._client_table_replicas.pop((table_name, client_id), [])
                )
                if not replicas:
                    replicas = list(range(self._jr_replicas))

            for replica in replicas:
                if not self.running:
                    logging.warning(
                        "Aggregator shutting down, aborting EOF forwarding for table=%s",
                        table_name,
                    )
                    raise Exception("Aggregator is shutting down")
                q = self._out_queues.get((table_name, replica))
                if not q:
                    raise Exception(
                        f"No out queue configured for table={table_name}, replica={replica}"
                    )
                q.send(raw)
                logging.debug(
                    "Forwarded EOF to joiner_router replica=%s table=%s trace=%s",
                    replica,
                    table_name,
                    original_trace,
                )
            if client_id:
                self._client_table_replicas.pop((table_name, client_id), None)
        except Exception:
            logging.exception("Failed to forward EOF")
            raise

    def _broadcast_cleanup_to_joiners(self, raw: bytes):
        """
        Broadcast client cleanup message to all joiner router replicas.

        Sends the cleanup message to all joiner router replicas using the
        menu_items table queue as a representative channel. This ensures
        all replicas receive the cleanup signal regardless of which table
        the cleanup message was received on.

        Args:
            raw (bytes): Raw serialized cleanup message bytes.

        Raises:
            Exception: If broadcasting cleanup message fails or aggregator
                is shutting down.
        """
        if not self.running:
            logging.warning(
                "Aggregator shutting down, refusing to broadcast cleanup message"
            )
            raise Exception("Aggregator is shutting down")
        try:
            for replica in range(self._jr_replicas):
                if not self.running:
                    logging.warning(
                        "Aggregator shutting down, aborting cleanup broadcast"
                    )
                    raise Exception("Aggregator is shutting down")
                queue = self._out_queues.get(
                    (TID_TO_NAME.get(TableName.MENU_ITEMS), replica)
                )
                queue.send(raw)
                logging.info(
                    "Forwarded CLEAN_UP_MESSAGE to joiner_router replica=%d",
                    replica,
                )
        except Exception:
            logging.exception("Failed to broadcast cleanup to joiners")
            raise

    def run(self):
        """
        Start the aggregator server.

        Begins consuming messages from all configured input exchanges for
        each table (menu_items, stores, transactions, transaction_items, users).
        Each exchange uses a dedicated handler method to process incoming messages.

        This method blocks and runs indefinitely until stop() is called.
        """
        self.running = True
        logging.info("Starting aggregator server with ID %s", self.id)

        self._exchanges["menu_items"].start_consuming(self._handle_menu_item)
        self._exchanges["stores"].start_consuming(self._handle_store)
        self._exchanges["transactions"].start_consuming(self._handle_transaction)
        self._exchanges["transaction_items"].start_consuming(
            self._handle_transaction_item
        )
        self._exchanges["users"].start_consuming(self._handle_user)

        logging.info("Started aggregator server")

    def stop(self):
        """Stop the aggregator server gracefully.

        This method performs a graceful shutdown by:
        1. Setting running = False to signal all handlers to stop processing
        2. Stopping consumption on all exchanges (waits for consumer threads to finish)
        3. Closing output queues (prevents new messages from being sent)
        4. Closing input exchanges (releases all connections)

        All message handlers check the running flag before processing and will
        refuse to process new messages or send to queues once shutdown begins.
        The middleware's stop_consuming() method waits for consumer threads to
        complete, ensuring in-flight messages are handled before shutdown completes.

        IMPORTANT: Order of operations matters to avoid race conditions:
        - Handlers check self.running at the start and during processing
        - stop_consuming() waits for consumer threads via join()
        - Queues are closed after consumption stops to prevent send errors
        """
        self.running = False
        logging.info("Stopping aggregator server")
        for exchange in self._exchanges.values():
            exchange.stop_consuming()

        for queue in self._out_queues.values():
            queue.close()

        for exchange in self._exchanges.values():
            exchange.close()

        logging.info("Aggregator server stopped")

    def _handle_menu_item(self, message: bytes) -> bool:
        """
        Handle incoming messages from the menu_items exchange.

        Processes three types of messages:
            - EOF_MESSAGE: Forwards to joiner routers
            - DATA_BATCH: Forwards to joiner routers without processing
            - CLEAN_UP_MESSAGE: Broadcasts to all joiner router replicas

        Args:
            message (bytes): Raw serialized message bytes.

        Returns:
            bool: True if message was handled successfully, False otherwise.
        """
        if not self.running:
            logging.warning(
                "Aggregator shutting down, refusing to handle menu item message"
            )
            return False
        logging.debug("Handling menu item message")
        try:
            if not message:
                return True
            envelope = Envelope()
            envelope.ParseFromString(message)
            if envelope.type == MessageType.EOF_MESSAGE:
                eof_msg = envelope.eof
                table_name = TID_TO_NAME[eof_msg.table]
                self._forward_eof(message, table_name)
            elif envelope.type == MessageType.DATA_BATCH:
                data_batch = envelope.data_batch
                table_name = TID_TO_NAME[data_batch.payload.name]
                self._forward_databatch_by_table(data_batch, message, table_name)
            elif envelope.type == MessageType.CLEAN_UP_MESSAGE:
                self._broadcast_cleanup_to_joiners(message)
            else:
                logging.warning("Unknown message type: %s", envelope.type)
            return True
        except Exception:
            logging.exception("Failed to handle menu item")
            return False

    def _handle_store(self, message: bytes) -> bool:
        """
        Handle incoming messages from the stores exchange.

        Processes three types of messages:
            - EOF_MESSAGE: Forwards to joiner routers
            - DATA_BATCH: Forwards to joiner routers without processing
            - CLEAN_UP_MESSAGE: Broadcasts to all joiner router replicas

        Args:
            message (bytes): Raw serialized message bytes.

        Returns:
            bool: True if message was handled successfully, False otherwise.
        """
        if not self.running:
            logging.warning(
                "Aggregator shutting down, refusing to handle store message"
            )
            return False
        logging.debug("Handling store message")
        try:
            if not message:
                return True
            envelope = Envelope()
            envelope.ParseFromString(message)
            if envelope.type == MessageType.EOF_MESSAGE:
                eof_msg = envelope.eof
                table_name = TID_TO_NAME[eof_msg.table]
                self._forward_eof(message, table_name)
            elif envelope.type == MessageType.DATA_BATCH:
                data_batch = envelope.data_batch
                table_name = TID_TO_NAME[data_batch.payload.name]
                self._forward_databatch_by_table(data_batch, message, table_name)
            elif envelope.type == MessageType.CLEAN_UP_MESSAGE:
                self._broadcast_cleanup_to_joiners(message)
            else:
                logging.warning("Unknown message type: %s", envelope.type)
            return True
        except Exception:
            logging.exception("Failed to handle store")
            return False

    def _handle_transaction(self, message: bytes) -> bool:
        """
        Handle incoming messages from the transactions exchange.

        Processes three types of messages:
            - EOF_MESSAGE: Forwards to joiner routers
            - DATA_BATCH: Processes based on query_id:
                * query_id 0 or None: Forwards without processing
                * query_id 2: Processes with process_query_3 and forwards
                * query_id 3: Processes with process_query_4_transactions and forwards
            - CLEAN_UP_MESSAGE: Broadcasts to all joiner router replicas

        Args:
            message (bytes): Raw serialized message bytes.

        Returns:
            bool: True if message was handled successfully, False otherwise.
        """
        if not self.running:
            logging.warning(
                "Aggregator shutting down, refusing to handle transaction message"
            )
            return False
        logging.debug("Handling transaction message")
        try:
            if not message:
                return False

            envelope = Envelope()
            envelope.ParseFromString(message)

            if envelope.type == MessageType.EOF_MESSAGE:
                eof_msg = envelope.eof
                table_name = TID_TO_NAME[eof_msg.table]
                self._forward_eof(message, table_name)
            elif envelope.type == MessageType.DATA_BATCH:
                data_batch = envelope.data_batch
                table_data = data_batch.payload
                rows = list(iterate_rows_as_dicts(table_data))
                query_id = data_batch.query_ids[0] if data_batch.query_ids else None

                if query_id == 0 or query_id is None:
                    self._forward_databatch_by_table(
                        data_batch, message, "transactions"
                    )
                elif query_id == 2:
                    if not self.running:
                        logging.warning(
                            "Aggregator shutting down, aborting query 3 processing"
                        )
                        return False
                    processed = process_query_3(rows)
                    out = serialize_query3_results(processed)
                    columns = [
                        "transaction_id",
                        "store_id",
                        "payment_method_id",
                        "user_id",
                        "original_amount",
                        "discount_applied",
                        "final_amount",
                        "created_at",
                    ]
                    row_values = [
                        [str(row.get(col, "")) for col in columns] for row in out
                    ]
                    new_table_data = build_table_data(
                        table_name=TableName.TRANSACTIONS,
                        columns=columns,
                        rows=row_values,
                        batch_number=table_data.batch_number,
                        status=table_data.status,
                    )
                    new_data_batch = DataBatch()
                    new_data_batch.CopyFrom(data_batch)
                    new_data_batch.payload.CopyFrom(new_table_data)
                    new_envelope = Envelope(
                        type=MessageType.DATA_BATCH, data_batch=new_data_batch
                    )
                    new_message = new_envelope.SerializeToString()
                    self._forward_databatch_by_table(
                        new_data_batch, new_message, "transactions"
                    )
                elif query_id == 3:
                    if not self.running:
                        logging.warning(
                            "Aggregator shutting down, aborting query 4 processing"
                        )
                        return False
                    processed = process_query_4_transactions(rows)
                    out = serialize_query4_transaction_results(processed)
                    columns = [
                        "transaction_id",
                        "store_id",
                        "payment_method_id",
                        "voucher_id",
                        "user_id",
                        "original_amount",
                        "discount_applied",
                        "final_amount",
                        "created_at",
                    ]
                    row_values = [
                        [str(row.get(col, "")) for col in columns] for row in out
                    ]
                    new_table_data = build_table_data(
                        table_name=TableName.TRANSACTIONS,
                        columns=columns,
                        rows=row_values,
                        batch_number=table_data.batch_number,
                        status=table_data.status,
                    )
                    new_data_batch = DataBatch()
                    new_data_batch.CopyFrom(data_batch)
                    new_data_batch.payload.CopyFrom(new_table_data)
                    new_envelope = Envelope(
                        type=MessageType.DATA_BATCH, data_batch=new_data_batch
                    )
                    new_message = new_envelope.SerializeToString()
                    self._forward_databatch_by_table(
                        new_data_batch, new_message, "transactions"
                    )
                else:
                    logging.error(
                        "Unexpected query_id=%s for transaction table", query_id
                    )
                    return True
            elif envelope.type == MessageType.CLEAN_UP_MESSAGE:
                self._broadcast_cleanup_to_joiners(message)
            else:
                logging.warning("Unknown message type: %s", envelope.type)
            return True
        except Exception:
            logging.exception("Failed to handle transaction")
            return False

    def _handle_transaction_item(self, message: bytes) -> bool:
        """
        Handle incoming messages from the transaction_items exchange.

        Processes three types of messages:
            - EOF_MESSAGE: Forwards to joiner routers
            - DATA_BATCH: Processes based on query_id:
                * query_id 1: Processes with process_query_2 and forwards
                * Other query_ids: Logs error and returns
            - CLEAN_UP_MESSAGE: Broadcasts to all joiner router replicas

        Args:
            message (bytes): Raw serialized message bytes.

        Returns:
            bool: True if message was handled successfully, False otherwise.
        """
        if not self.running:
            logging.warning(
                "Aggregator shutting down, refusing to handle transaction item message"
            )
            return False
        logging.debug("Handling transaction item message")
        try:
            if not message:
                return True

            envelope = Envelope()
            envelope.ParseFromString(message)

            if envelope.type == MessageType.EOF_MESSAGE:
                eof_msg = envelope.eof
                table_name = TID_TO_NAME[eof_msg.table]
                self._forward_eof(message, table_name)

            elif envelope.type == MessageType.DATA_BATCH:
                data_batch = envelope.data_batch
                table_data = data_batch.payload
                rows = list(iterate_rows_as_dicts(table_data))
                query_id = data_batch.query_ids[0] if data_batch.query_ids else None

                if int(query_id) == 1:
                    if not self.running:
                        logging.warning(
                            "Aggregator shutting down, aborting query 2 processing"
                        )
                        return False
                    logging.debug("Processing transaction item with query 2")
                    processed = process_query_2(rows)
                    out = serialize_query2_results(processed)
                    columns = [
                        "transaction_item_id",
                        "transaction_id",
                        "item_id",
                        "quantity",
                        "subtotal",
                        "created_at",
                    ]
                    row_values = [
                        [str(row.get(col, "")) for col in columns] for row in out
                    ]
                    status = table_data.status
                    new_table_data = build_table_data(
                        table_name=TableName.TRANSACTION_ITEMS,
                        columns=columns,
                        rows=row_values,
                        batch_number=table_data.batch_number,
                        status=status,
                    )
                    new_data_batch = DataBatch(
                        query_ids=data_batch.query_ids,
                        filter_steps=data_batch.filter_steps,
                        shards_info=data_batch.shards_info,
                        client_id=data_batch.client_id,
                        payload=new_table_data,
                    )
                    new_envelope = Envelope(
                        type=MessageType.DATA_BATCH, data_batch=new_data_batch
                    )
                    new_message = new_envelope.SerializeToString()
                    self._forward_databatch_by_table(
                        new_data_batch, new_message, "transaction_items"
                    )
                else:
                    logging.error(
                        "Transaction item with query distinct from 1: %s", query_id
                    )
                    return True
            elif envelope.type == MessageType.CLEAN_UP_MESSAGE:
                self._broadcast_cleanup_to_joiners(message)
            else:
                logging.warning("Unknown message type: %s", envelope.type)
            return True
        except Exception:
            logging.exception("Failed to handle transaction item")
            return False

    def _handle_user(self, message: bytes) -> bool:
        """
        Handle incoming messages from the users exchange.

        Processes three types of messages:
            - EOF_MESSAGE: Forwards to joiner routers
            - DATA_BATCH: Forwards to joiner routers without processing
            - CLEAN_UP_MESSAGE: Broadcasts to all joiner router replicas

        Args:
            message (bytes): Raw serialized message bytes.

        Returns:
            bool: True if message was handled successfully, False otherwise.
        """
        if not self.running:
            logging.warning(
                "Aggregator shutting down, refusing to handle user message"
            )
            return False
        logging.debug("Handling user message")
        try:
            if not message:
                logging.error("Empty message received in user handler")
                return True
            envelope = Envelope()
            envelope.ParseFromString(message)
            if envelope.type == MessageType.EOF_MESSAGE:
                eof_msg = envelope.eof
                table_name = TID_TO_NAME[eof_msg.table]
                self._forward_eof(message, table_name)
            elif envelope.type == MessageType.DATA_BATCH:
                data_batch = envelope.data_batch
                table_name = TID_TO_NAME[data_batch.payload.name]
                self._forward_databatch_by_table(data_batch, message, table_name)
            elif envelope.type == MessageType.CLEAN_UP_MESSAGE:
                self._broadcast_cleanup_to_joiners(message)
            else:
                logging.warning("Unknown message type: %s", envelope.type)
            return True
        except Exception:
            logging.exception("Failed to handle user")
            return False
