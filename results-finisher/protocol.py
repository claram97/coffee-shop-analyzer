import socket
import io
from typing import List, Dict, Any

class ProtocolError(Exception):
    """Represents a framing/validation error while parsing or writing messages."""
    def __init__(self, message, opcode=None):
        super().__init__(message)
        self.opcode = opcode

class Opcodes:
    NEW_BETS = 0
    BETS_RECV_SUCCESS = 1
    BETS_RECV_FAIL = 2
    FINISHED = 3
    NEW_MENU_ITEMS = 4
    NEW_PAYMENT_METHODS = 5
    NEW_STORES = 6
    NEW_TRANSACTION_ITEMS = 7
    NEW_TRANSACTION = 8
    NEW_USERS = 9
    NEW_VOUCHERS = 10
    DATA_BATCH = 11

# --- RAW DATA CLASSES ---
class RawBet:
    """Transport-level bet structure read from the wire (not the domain model)."""
    def __init__(self, first_name: str, last_name: str, document: str, birthdate: str, number: str):
        self.first_name = first_name
        self.last_name = last_name
        self.document = document
        self.birthdate = birthdate
        self.number = number

class RawMenuItems:
    def __init__(self, product_id: str, name: str, price: str, category: str, is_seasonal: str, available_from: str, available_to: str):
        self.product_id, self.name, self.price, self.category = product_id, name, price, category
        self.is_seasonal, self.available_from, self.available_to = is_seasonal, available_from, available_to

class RawPaymentMethod:
    def __init__(self, method_id: str, method_name: str, category: str):
        self.method_id, self.method_name, self.category = method_id, method_name, category

class RawStore:
    def __init__(self, store_id: str, store_name: str, street: str, postal_code: str, city: str, state: str, latitude: str, longitude: str):
        self.store_id, self.store_name, self.street, self.postal_code = store_id, store_name, street, postal_code
        self.city, self.state, self.latitude, self.longitude = city, state, latitude, longitude

class RawTransactionItem:
    def __init__(self, transaction_id: str, item_id: str, quantity: str, unit_price: str, subtotal: str, created_at: str):
        self.transaction_id, self.item_id, self.quantity = transaction_id, item_id, quantity
        self.unit_price, self.subtotal, self.created_at = unit_price, subtotal, created_at

class RawTransaction:
    def __init__(self, transaction_id: str, store_id: str, payment_method_id: str, voucher_id: str, user_id: str, original_amount: str, discount_applied: str, final_amount: str, created_at: str):
        self.transaction_id, self.store_id, self.payment_method_id = transaction_id, store_id, payment_method_id
        self.voucher_id, self.user_id, self.original_amount = voucher_id, user_id, original_amount
        self.discount_applied, self.final_amount, self.created_at = discount_applied, final_amount, created_at

class RawUser:
    def __init__(self, user_id: str, gender: str, birthdate: str, registered_at: str):
        self.user_id, self.gender, self.birthdate, self.registered_at = user_id, gender, birthdate, registered_at

class RawVoucher:
    def __init__(self, voucher_id: str, voucher_code: str, discount_type: str, discount_value: str, valid_from: str, valid_to: str):
        self.voucher_id, self.voucher_code, self.discount_type = voucher_id, voucher_code, discount_type
        self.discount_value, self.valid_from, self.valid_to = discount_value, valid_from, valid_to

class TableMessage:
    """Generic base class to read and write messages containing tabular data."""
    def __init__(self, opcode: int, required_keys: tuple[str, ...], row_factory):
        self.opcode = opcode
        self.required_keys = required_keys
        self.rows = []
        self.amount = 0
        self._row_factory = row_factory

    def __read_row(self, sock: io.BytesIO, remaining: int) -> int:
        current_row_data: dict[str, str] = {}
        (n_pairs, remaining) = read_i32(sock, remaining, self.opcode)
        if n_pairs != len(self.required_keys):
            raise ProtocolError(f"Expected {len(self.required_keys)} pairs, got {n_pairs}", self.opcode)
        
        for _ in range(n_pairs):
            (key, remaining) = read_string(sock, remaining, self.opcode)
            (value, remaining) = read_string(sock, remaining, self.opcode)
            current_row_data[key] = value

        missing_keys = [key for key in self.required_keys if key not in current_row_data]
        if missing_keys:
            raise ProtocolError(f"Missing required keys: {missing_keys}", self.opcode)
            
        if self._row_factory == RawBet:
            mapping = {"NOMBRE": "first_name", "APELLIDO": "last_name", "DOCUMENTO": "document", "NACIMIENTO": "birthdate", "NUMERO": "number"}
            kwargs = {mapping.get(k, k): v for k, v in current_row_data.items()}
        else:
            kwargs = {key.lower(): value for key, value in current_row_data.items()}
            
        self.rows.append(self._row_factory(**kwargs))
        return remaining

    def read_from(self, sock: io.BytesIO, length: int):
        remaining = length
        try:
            (n_rows, remaining) = read_i32(sock, remaining, self.opcode)
            self.amount = n_rows
            for i in range(n_rows):
                remaining = self.__read_row(sock, remaining)
            if remaining != 0:
                raise ProtocolError(f"Indicated length doesn't match body length: expected 0, got {remaining}", self.opcode)
        except ProtocolError:
            if remaining > 0:
                recv_exactly(sock, remaining)
            raise

    def _write_row(self, stream, row_obj: Any):
        """Writes a single data object (row) to the provided stream."""
        if self._row_factory == RawBet:
            mapping = {"first_name": "NOMBRE", "last_name": "APELLIDO", "document": "DOCUMENTO", "birthdate": "NACIMIENTO", "number": "NUMERO"}
            row_data = {mapping.get(k, k): v for k, v in vars(row_obj).items()}
        else:
            row_data = vars(row_obj)
        
        write_i32(stream, len(row_data))
        for key, value in row_data.items():
            write_string(stream, str(key))
            write_string(stream, str(value))

    def get_body_bytes(self) -> bytes:
        """Serializes the entire table (all rows) into a byte string."""
        buffer = io.BytesIO()
        write_i32(buffer, len(self.rows))
        for row in self.rows:
            self._write_row(buffer, row)
        return buffer.getvalue()

class NewBets(TableMessage):
    def __init__(self):
        required = ("NOMBRE", "APELLIDO", "DOCUMENTO", "NACIMIENTO", "NUMERO")
        super().__init__(opcode=Opcodes.NEW_BETS, required_keys=required, row_factory=RawBet)

class NewMenuItems(TableMessage):
    def __init__(self):
        required = ("product_id", "name", "category", "price", "is_seasonal", "available_from", "available_to")
        super().__init__(opcode=Opcodes.NEW_MENU_ITEMS, required_keys=required, row_factory=RawMenuItems)

class NewTransactionItems(TableMessage):
    def __init__(self):
        required = ("transaction_id", "item_id", "quantity", "unit_price", "subtotal", "created_at")
        super().__init__(opcode=Opcodes.NEW_TRANSACTION_ITEMS, required_keys=required, row_factory=RawTransactionItem)

class NewTransactions(TableMessage):
    def __init__(self):
        required = ("transaction_id", "store_id", "payment_method_id", "voucher_id", "user_id", "original_amount", "discount_applied", "final_amount", "created_at")
        super().__init__(opcode=Opcodes.NEW_TRANSACTION, required_keys=required, row_factory=RawTransaction)

class NewUsers(TableMessage):
    def __init__(self):
        required = ("user_id", "gender", "birthdate", "registered_at")
        super().__init__(opcode=Opcodes.NEW_USERS, required_keys=required, row_factory=RawUser)

class NewVouchers(TableMessage):
    def __init__(self):
        required = ("voucher_id", "voucher_code", "discount_type", "discount_value", "valid_from", "valid_to")
        super().__init__(opcode=Opcodes.NEW_VOUCHERS, required_keys=required, row_factory=RawVoucher)

class NewPaymentMethods(TableMessage):
    def __init__(self):
        required = ("method_id", "method_name", "category")
        super().__init__(opcode=Opcodes.NEW_PAYMENT_METHODS, required_keys=required, row_factory=RawPaymentMethod)

class NewStores(TableMessage):
    def __init__(self):
        required = ("store_id", "store_name", "street", "postal_code", "city", "state", "latitude", "longitude")
        super().__init__(opcode=Opcodes.NEW_STORES, required_keys=required, row_factory=RawStore)

class Finished:
    def __init__(self):
        self.opcode = Opcodes.FINISHED
        self.agency_id = None
    def read_from(self, sock: io.BytesIO, length: int):
        if length != 4:
            raise ProtocolError("invalid length for FINISHED message", self.opcode)
        (agency_id, _) = read_i32(sock, length, self.opcode)
        self.agency_id = agency_id

def recv_exactly(sock: io.BytesIO, n: int) -> bytes:
    if n < 0: raise ProtocolError("invalid body")
    data = sock.read(n)
    if len(data) < n: raise EOFError("socket closed or short read")
    return data

def read_u8(sock: io.BytesIO) -> int: return recv_exactly(sock, 1)[0]
def read_u16(sock: io.BytesIO, rem: int, op: int) -> tuple[int, int]:
    if rem < 2: raise ProtocolError("indicated length too short", op)
    return int.from_bytes(recv_exactly(sock, 2), "little", signed=False), rem - 2
def read_i32(sock: io.BytesIO, rem: int, op: int) -> tuple[int, int]:
    if rem < 4: raise ProtocolError("indicated length too short", op)
    return int.from_bytes(recv_exactly(sock, 4), "little", signed=True), rem - 4
def read_i64(sock: io.BytesIO, rem: int, op: int) -> tuple[int, int]:
    if rem < 8: raise ProtocolError("indicated length too short", op)
    return int.from_bytes(recv_exactly(sock, 8), "little", signed=True), rem - 8
def read_string(sock: io.BytesIO, rem: int, op: int) -> tuple[str, int]:
    (ln, rem) = read_i32(sock, rem, op)
    if rem < ln: raise ProtocolError("indicated length too short for string", op)
    s = recv_exactly(sock, ln).decode("utf-8")
    return s, rem - ln

def write_u8(stream, value: int) -> None:
    stream.write(bytes([value]))
def write_u16(stream, value: int) -> None:
    stream.write(int(value).to_bytes(2, "little", signed=False))
def write_i32(stream, value: int) -> None:
    stream.write(int(value).to_bytes(4, "little", signed=True))
def write_i64(stream, value: int) -> None:
    stream.write(int(value).to_bytes(8, "little", signed=True))
def write_string(stream, s: str) -> None:
    b = s.encode("utf-8")
    write_i32(stream, len(b))
    stream.write(b)

def _read_u8_with_remaining(sock: io.BytesIO, rem: int, op: int) -> tuple[int, int]:
    """Read a single u8 and update remaining count."""
    if rem < 1:
        raise ProtocolError("Not enough data to read u8", op)
    value = sock.read(1)[0]
    return value, rem - 1

def _read_u16_with_remaining(sock: io.BytesIO, rem: int, op: int) -> tuple[int, int]:
    """Read a single u16 and update remaining count."""
    if rem < 2:
        raise ProtocolError("Not enough data to read u16", op)
    value_bytes = sock.read(2)
    value = int.from_bytes(value_bytes, byteorder="little", signed=False)
    return value, rem - 2

def _read_u8_list_from_body(body: bytes, offset: int) -> tuple[List[int], int]:
    """Helper to read a u8 list (count + items) from a byte string."""
    if offset >= len(body):
        raise ProtocolError("Not enough data to read list count.")
    count = body[offset]
    offset += 1
    
    if offset + count > len(body):
        raise ProtocolError("Not enough data to read list items.")
    
    items = list(body[offset : offset + count])
    offset += count
    return items, offset

def _read_u8_u8_dict_from_body(body: bytes, offset: int) -> tuple[Dict[int, int], int]:
    """Helper to read a u8->u8 dictionary (count + key-value pairs) from body."""
    if offset >= len(body):
        raise ProtocolError("Not enough data to read dict count")
    count = body[offset]
    offset += 1
    
    result = {}
    for _ in range(count):
        if offset + 2 > len(body):
            raise ProtocolError("Not enough data to read dict pair")
        key = body[offset]
        value = body[offset + 1]
        result[key] = value
        offset += 2
    
    return result, offset
def _read_u8_list(sock: io.BytesIO, remaining: int, opcode: int) -> tuple[List[int], int]:
    """Helper to read a u8 list (count + items) from a byte string."""
    if remaining < 1:
        raise ProtocolError("Not enough data to read list count", opcode)
    count = sock.read(1)[0]
    remaining -= 1
    
    if remaining < count:
        raise ProtocolError("Not enough data to read list items", opcode)
    
    items = []
    for _ in range(count):
        items.append(sock.read(1)[0])
        remaining -= 1
    
    return items, remaining
def _read_u8_u8_dict(sock: io.BytesIO, remaining: int, opcode: int) -> tuple[Dict[int, int], int]:
    """Helper to read a u8->u8 dictionary (count + key-value pairs)."""
    if remaining < 1:
        raise ProtocolError("Not enough data to read dict count", opcode)
    count = sock.read(1)[0]
    remaining -= 1
    
    result = {}
    for _ in range(count):
        if remaining < 2:
            raise ProtocolError("Not enough data to read dict pair", opcode)
        key = sock.read(1)[0]
        value = sock.read(1)[0]
        result[key] = value
        remaining -= 2
    
    return result, remaining

def _instantiate_message_for_opcode(opcode: int):
    """Factory function to create message instances based on opcode."""
    if opcode == Opcodes.NEW_BETS:
        return NewBets()
    elif opcode == Opcodes.NEW_MENU_ITEMS:
        return NewMenuItems()
    elif opcode == Opcodes.NEW_PAYMENT_METHODS:
        return NewPaymentMethods()
    elif opcode == Opcodes.NEW_STORES:
        return NewStores()
    elif opcode == Opcodes.NEW_TRANSACTION_ITEMS:
        return NewTransactionItems()
    elif opcode == Opcodes.NEW_TRANSACTION:
        return NewTransactions()
    elif opcode == Opcodes.NEW_USERS:
        return NewUsers()
    elif opcode == Opcodes.NEW_VOUCHERS:
        return NewVouchers()
    elif opcode == Opcodes.FINISHED:
        return Finished()
    else:
        # Return a generic TableMessage for unknown opcodes
        return TableMessage(opcode, (), dict)

class DataBatch:
    """Represents a DataBatch message containing batched data and metadata."""
    def __init__(self, table_ids=None, query_ids=None, batch_number=0, meta=None, total_shards=1, shard_num=0, batch_msg=None):
        self.table_ids = table_ids or []
        self.query_ids = query_ids or []
        self.batch_number = batch_number
        self.meta = meta or {}
        self.total_shards = total_shards
        self.shard_num = shard_num
        self.batch_msg = batch_msg

    def read_from(self, sock: io.BytesIO, length: int):
        """Parse the DataBatch body from the socket."""
        remaining = length
        
        # Read table_ids (u8 list)
        self.table_ids, remaining = _read_u8_list(sock, remaining, Opcodes.DATA_BATCH)
        
        # Read query_ids (u8 list)  
        self.query_ids, remaining = _read_u8_list(sock, remaining, Opcodes.DATA_BATCH)
        
        # Read reserved field (u16)
        if remaining < 2:
            raise ProtocolError(f"Not enough data to read reserved field: need 2, have {remaining}", Opcodes.DATA_BATCH)
        reserved_bytes = sock.read(2)
        remaining -= 2
        
        # Read batch_number (i64, not i32!)
        if remaining < 8:
            raise ProtocolError(f"Not enough data to read batch_number: need 8, have {remaining}", Opcodes.DATA_BATCH)
        batch_number_bytes = sock.read(8)
        self.batch_number = int.from_bytes(batch_number_bytes, byteorder="little", signed=True)
        remaining -= 8
        
        # Read meta (u8->u8 dict)
        self.meta, remaining = _read_u8_u8_dict(sock, remaining, Opcodes.DATA_BATCH)
        
        # Read total_shards (u16, not u8!)
        if remaining < 2:
            raise ProtocolError(f"Not enough data to read total_shards: need 2, have {remaining}", Opcodes.DATA_BATCH)
        total_shards_bytes = sock.read(2)
        self.total_shards = int.from_bytes(total_shards_bytes, byteorder="little", signed=False)
        remaining -= 2
        
        # Read shard_num (u16, not u8!)
        if remaining < 2:
            raise ProtocolError(f"Not enough data to read shard_num: need 2, have {remaining}", Opcodes.DATA_BATCH)
        shard_num_bytes = sock.read(2)
        self.shard_num = int.from_bytes(shard_num_bytes, byteorder="little", signed=False)
        remaining -= 2
        
        # The remaining data is the inner message with its own framing
        # Read the inner message opcode
        if remaining < 1:
            raise ProtocolError(f"Not enough data to read inner opcode: need 1, have {remaining}", Opcodes.DATA_BATCH)
        inner_opcode = sock.read(1)[0]
        remaining -= 1
        
        # Read inner message length
        if remaining < 4:
            raise ProtocolError(f"Not enough data to read inner length: need 4, have {remaining}", Opcodes.DATA_BATCH)
        inner_length_bytes = sock.read(4)
        inner_length = int.from_bytes(inner_length_bytes, byteorder="little", signed=True)
        remaining -= 4
        
        if remaining != inner_length:
            raise ProtocolError(f"Inner message length mismatch: expected {inner_length}, got {remaining} remaining bytes", Opcodes.DATA_BATCH)
        
        # Instantiate and parse the inner message
        self.batch_msg = _instantiate_message_for_opcode(inner_opcode)
        self.batch_msg.read_from(sock, inner_length)

def deserialize_message(body: bytes) -> DataBatch:
    """
    Deserialize a binary message into a DataBatch object.
    """
    if len(body) < 5:
        raise ProtocolError("Message too short to contain outer frame")
    
    # Check opcode
    opcode = body[0]
    if opcode != Opcodes.DATA_BATCH:
        raise ProtocolError(f"Expected DATA_BATCH opcode, got {opcode}")
    
    # Read length from outer frame
    length = int.from_bytes(body[1:5], byteorder="little", signed=True)
    
    if len(body) != 5 + length:  # opcode + length + body
        raise ProtocolError(f"Message length mismatch: expected {5 + length}, got {len(body)}")
    
    # Use the existing DataBatch.read_from method to properly parse the message
    inner_body = body[5:]  # Skip outer frame
    sock = io.BytesIO(inner_body)
    
    data_batch = DataBatch()
    data_batch.read_from(sock, length)
    
    return data_batch