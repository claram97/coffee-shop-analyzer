import socket
import struct
import logging
from typing import Optional, List, Dict


# Protocol limits
MAX_BATCH_SIZE_BYTES = 90 * 1024 * 1024  # 90MB - Límite máximo del tamaño de batch en bytes


class ProtocolError(Exception):
    """Represents a framing/validation error while parsing or writing messages.

    `opcode` optionally identifies the message context in which the error occurred.
    """

    def __init__(self, message, opcode=None):
        super().__init__(message)
        self.opcode = opcode


class Opcodes:
    DATA_BATCH = 0 # Envuelve a los mensajes de tabla
    BETS_RECV_SUCCESS = 1
    BETS_RECV_FAIL = 2
    FINISHED = 3
    NEW_MENU_ITEMS = 4
    NEW_STORES = 5
    NEW_TRANSACTION_ITEMS = 6
    NEW_TRANSACTION = 7
    NEW_USERS = 8


class BatchStatus:
    """Status values for batch messages."""
    CONTINUE = 0  # Hay más batches en el archivo
    EOF = 1       # Último batch del archivo
    CANCEL = 2    # Batch enviado por cancelación


class RawMenuItems:
    def __init__(
        self,
        product_id: str,
        name: str,
        price: str,
        category: str,
        is_seasonal: str,
        available_from: str,
        available_to: str,
    ):
        self.product_id = product_id
        self.name = name
        self.price = price
        self.category = category
        self.is_seasonal = is_seasonal
        self.available_from = available_from
        self.available_to = available_to


class RawStore:
    def __init__(
        self,
        store_id: str,
        store_name: str,
        street: str,
        postal_code: str,
        city: str,
        state: str,
        latitude: str,
        longitude: str,
    ):
        self.store_id = store_id
        self.store_name = store_name
        self.street = street
        self.postal_code = postal_code
        self.city = city
        self.state = state
        self.latitude = latitude
        self.longitude = longitude


class RawTransactionItem:
    def __init__(
        self,
        transaction_id: str,
        item_id: str,
        quantity: str,
        unit_price: str,
        subtotal: str,
        created_at: str,
    ):
        self.transaction_id = transaction_id
        self.item_id = item_id
        self.quantity = quantity
        self.unit_price = unit_price
        self.subtotal = subtotal
        self.created_at = created_at


class RawTransaction:
    def __init__(
        self,
        transaction_id: str,
        store_id: str,
        payment_method_id: str,
        user_id: str,
        original_amount: str,
        discount_applied: str,
        final_amount: str,
        created_at: str,
    ):
        self.transaction_id = transaction_id
        self.store_id = store_id
        self.payment_method_id = payment_method_id
        self.user_id = user_id
        self.original_amount = original_amount
        self.discount_applied = discount_applied
        self.final_amount = final_amount
        self.created_at = created_at


class RawUser:
    def __init__(self, user_id: str, gender: str, birthdate: str, registered_at: str):
        self.user_id = user_id
        self.gender = gender
        self.birthdate = birthdate
        self.registered_at = registered_at


class TableMessage:
    """
    Clase base genérica para leer un mensaje que contiene una tabla de datos.
    Una tabla es una lista de registros, donde cada registro es un mapa de
    clave-valor.
    """

    def __init__(self, opcode: int, required_keys: tuple[str, ...], row_factory):
        self.opcode = opcode
        self.required_keys = required_keys
        self.rows = []  # Almacenará los objetos creados (e.g., RawMenuItems, RawStore)
        self.amount = 0
        self.batch_number = 0  # Número de batch del cliente
        self.batch_status = 0  # NUEVO: Status del batch (Continue/EOF/Cancel)
        # `row_factory` es una función o clase que convierte un dict en un objeto
        self._row_factory = row_factory

    def __read_row(self, sock: socket.socket, remaining: int) -> int:
        """Lee un único registro (fila) de la tabla."""
        current_row_data: dict[str, str] = {}
        (n_pairs, remaining) = read_i32(sock, remaining, self.opcode)

        # La validación ahora es genérica
        if n_pairs != len(self.required_keys):
            raise ProtocolError(
                f"Expected {len(self.required_keys)} pairs, got {n_pairs}", self.opcode
            )

        for _ in range(n_pairs):
            (key, remaining) = read_string(sock, remaining, self.opcode)
            (value, remaining) = read_string(sock, remaining, self.opcode)
            current_row_data[key] = value

        # Verificación de claves genérica
        missing_keys = [
            key for key in self.required_keys if key not in current_row_data
        ]
        if missing_keys:
            received_keys = list(current_row_data.keys())
            raise ProtocolError(
                f"Missing required keys: {missing_keys}. Received keys: {received_keys}. Expected: {list(self.required_keys)}",
                self.opcode,
            )

        # Por defecto: claves a minúsculas
        kwargs = {key.lower(): value for key, value in current_row_data.items()}
        self.rows.append(self._row_factory(**kwargs))
        return remaining

    def read_from(self, sock: socket.socket, length: int):
        """Parsea el cuerpo completo del mensaje de la tabla.
        
        Formato del mensaje del cliente:
        [length:i32][nRows:i32][batchNumber:i64][status:u8][rows...]
        """
        remaining = length
        try:
            # Leer número de filas
            (n_rows, remaining) = read_i32(sock, remaining, self.opcode)
            self.amount = n_rows
            
            # Leer número de batch
            (batch_number, remaining) = read_i64(sock, remaining, self.opcode)
            self.batch_number = batch_number
            
            # NUEVO: Leer status del batch
            (batch_status, remaining) = _read_u8_with_remaining(sock, remaining, self.opcode)
            self.batch_status = batch_status
            
            # Leer todas las filas
            for _ in range(n_rows):
                remaining = self.__read_row(sock, remaining)

            if remaining != 0:
                raise ProtocolError(
                    "Indicated length doesn't match body length", self.opcode
                )
        except ProtocolError:
            if remaining > 0:
                _ = recv_exactly(sock, remaining)
            raise


# --- CLASES DE MENSAJES ESPECÍFICOS (Ahora mucho más simples) ---


class NewMenuItems(TableMessage):
    """Mensaje NEW_PRODUCTS que ahora hereda de TableMessage."""

    def __init__(self):
        required = (
            "product_id",
            "name",
            "category",
            "price",
            "is_seasonal",
            "available_from",
            "available_to",
        )
        super().__init__(
            opcode=Opcodes.NEW_MENU_ITEMS,
            required_keys=required,
            row_factory=RawMenuItems,  # Usará RawMenuItems(**kwargs) para crear los objetos
        )


class NewTransactionItems(TableMessage):
    """Mensaje NEW_TRANSACTION_ITEMS que ahora hereda de TableMessage."""

    def __init__(self):
        required = (
            "transaction_id",
            "item_id",
            "quantity",
            "unit_price",
            "subtotal",
            "created_at",
        )
        super().__init__(
            opcode=Opcodes.NEW_TRANSACTION_ITEMS,
            required_keys=required,
            row_factory=RawTransactionItem,  # Usará RawTransactionItem(**kwargs) para crear los objetos
        )


class NewTransactions(TableMessage):
    """Mensaje NEW_TRANSACTIONS que ahora hereda de TableMessage."""

    def __init__(self):
        required = (
            "transaction_id",
            "store_id",
            "payment_method_id",
            "user_id",
            "original_amount",
            "discount_applied",
            "final_amount",
            "created_at",
        )
        super().__init__(
            opcode=Opcodes.NEW_TRANSACTION,
            required_keys=required,
            row_factory=RawTransaction,  # Usará RawTransaction(**kwargs) para crear los objetos
        )


class NewUsers(TableMessage):
    """Mensaje NEW_USERS que ahora hereda de TableMessage."""

    def __init__(self):
        required = ("user_id", "gender", "birthdate", "registered_at")
        super().__init__(
            opcode=Opcodes.NEW_USERS,
            required_keys=required,
            row_factory=RawUser,  # Usará RawUser(**kwargs) para crear los objetos
        )


class NewStores(TableMessage):
    """Mensaje NEW_STORES que ahora hereda de TableMessage."""

    def __init__(self):
        required = (
            "store_id",
            "store_name",
            "street",
            "postal_code",
            "city",
            "state",
            "latitude",
            "longitude",
        )
        super().__init__(
            opcode=Opcodes.NEW_STORES,
            required_keys=required,
            row_factory=RawStore,  # Usará RawStore(**kwargs) para crear los objetos
        )


class Finished:
    """Inbound FINISHED message. Body is a single agency_id (i32 LE)."""

    def __init__(self):
        self.opcode = Opcodes.FINISHED
        self.agency_id = None
        self._length = 4

    def read_from(self, sock: socket.socket, length: int):
        """Validate fixed body length (4) and read agency_id."""
        if length != self._length:
            raise ProtocolError("invalid length", self.opcode)
        (agency_id, _) = read_i32(sock, length, self.opcode)
        self.agency_id = agency_id


def recv_exactly(sock: socket.socket, n: int) -> bytes:
    """Read exactly n bytes (retrying as needed) or raise EOFError on peer close.

    Converts timeouts/OS errors to ProtocolError. Prevents short reads.
    """
    if n < 0:
        raise ProtocolError("invalid body")
    data = bytearray(n)
    view = memoryview(data)
    read = 0
    while read < n:
        try:
            nrecv = sock.recv_into(view[read:], n - read)
        except socket.timeout as e:
            raise ProtocolError("recv timeout") from e
        except InterruptedError:
            continue
        except OSError as e:
            raise ProtocolError(f"recv failed: {e}") from e
        if nrecv == 0:
            raise EOFError("peer closed connection")
        read += nrecv
    return bytes(data)


def read_u8(sock: socket.socket) -> int:
    """Read one unsigned byte (u8)."""
    return recv_exactly(sock, 1)[0]


def read_u16(sock: socket.socket, remaining: int, opcode: int) -> tuple[int, int]:
    """Lee un u16 little-endian y descuenta remaining."""
    if remaining < 2:
        raise ProtocolError("indicated length doesn't match body length", opcode)
    remaining -= 2
    val = int.from_bytes(recv_exactly(sock, 2), byteorder="little", signed=False)
    return val, remaining


def read_i32(sock: socket.socket, remaining: int, opcode: int) -> tuple[int, int]:
    """Read a little-endian signed int32 and decrement `remaining` accordingly.

    Raises ProtocolError if fewer than 4 bytes remain to be read.
    """
    if remaining < 4:
        raise ProtocolError("indicated length doesn't match body length", opcode)
    remaining -= 4
    val = int.from_bytes(recv_exactly(sock, 4), byteorder="little", signed=True)
    return val, remaining


def read_i64(sock: socket.socket, remaining: int, opcode: int) -> tuple[int, int]:
    """Lee un i64 little-endian y descuenta remaining."""
    if remaining < 8:
        raise ProtocolError("indicated length doesn't match body length", opcode)
    remaining -= 8
    val = int.from_bytes(recv_exactly(sock, 8), byteorder="little", signed=True)
    return val, remaining


def read_string(sock: socket.socket, remaining: int, opcode: int) -> tuple[str, int]:
    """Read a protocol [string]: i32 length (validated) + UTF-8 bytes.

    Ensures a strictly positive length and sufficient remaining payload.
    Returns the decoded string and the updated `remaining`.
    """
    (key_len, remaining) = read_i32(sock, remaining, opcode)
    if key_len < 0:
        raise ProtocolError("invalid body", opcode)
    if key_len == 0:
        return ("", remaining)
    if remaining < key_len:
        raise ProtocolError("indicated length doesn't match body length", opcode)
    try:
        s = recv_exactly(sock, key_len).decode("utf-8")
    except UnicodeDecodeError as e:
        raise ProtocolError("invalid body", opcode) from e
    remaining -= key_len
    return (s, remaining)


def recv_msg(sock: socket.socket):
    """Lee un mensaje y lo despacha a la clase apropiada."""
    opcode = read_u8(sock)
    (length, _) = read_i32(sock, 4, -1)
    if length < 0:
        raise ProtocolError("invalid length")

    msg = None
    if opcode == Opcodes.FINISHED:
        msg = Finished()
    elif opcode == Opcodes.NEW_MENU_ITEMS:
        msg = NewMenuItems()
    elif opcode == Opcodes.NEW_STORES:
        msg = NewStores()
    elif opcode == Opcodes.NEW_TRANSACTION_ITEMS:
        msg = NewTransactionItems()
    elif opcode == Opcodes.NEW_TRANSACTION:
        msg = NewTransactions()
    elif opcode == Opcodes.NEW_USERS:
        msg = NewUsers()
    elif opcode == Opcodes.DATA_BATCH:
        msg = DataBatch()
    else:
        raise ProtocolError(f"invalid opcode: {opcode}")

    msg.read_from(sock, length)
    return msg


def write_u8(sock, value: int) -> None:
    """Write a single unsigned byte (u8) using sendall()."""
    if not 0 <= value <= 255:
        raise ValueError("u8 out of range")
    sock.sendall(bytes([value]))


def write_u16(sock: socket.socket, value: int) -> None:
    if not 0 <= int(value) <= 0xFFFF:
        raise ValueError("u16 out of range")
    sock.sendall(int(value).to_bytes(2, byteorder="little", signed=False))


def write_i32(sock: socket.socket, value: int) -> None:
    """Write a little-endian signed int32 using sendall()."""
    sock.sendall(int(value).to_bytes(4, byteorder="little", signed=True))


def write_i64(sock: socket.socket, value: int) -> None:
    sock.sendall(int(value).to_bytes(8, byteorder="little", signed=True))


def write_string(sock: socket.socket, s: str) -> None:
    """Write a protocol [string]: i32 length prefix + UTF-8 bytes."""
    b = s.encode("utf-8")
    n = len(b)
    write_i32(sock, n)
    sock.sendall(b)


class BetsRecvSuccess:
    """Outbound BETS_RECV_SUCCESS response (empty body)."""

    def __init__(self):
        self.opcode = Opcodes.BETS_RECV_SUCCESS

    def write_to(self, sock: socket.socket):
        """Frame and send the success response: [opcode][length=0]."""
        write_u8(sock, self.opcode)
        write_i32(sock, 0)


class BetsRecvFail:
    """Outbound BETS_RECV_FAIL response (empty body)."""

    def __init__(self):
        self.opcode = Opcodes.BETS_RECV_FAIL

    def write_to(self, sock: socket.socket):
        """Frame and send the failure response: [opcode][length=0]."""
        write_u8(sock, self.opcode)
        write_i32(sock, 0)


# [opcode] [lista de identificadores de tablas] [lista de identificadores de queries] [u16] [numero de batch] [diccionario 1] [total shards] [num shard] [batch]


def _read_u8_with_remaining(
    sock: socket.socket, remaining: int, opcode: int
) -> tuple[int, int]:
    if remaining < 1:
        raise ProtocolError("indicated length doesn't match body length", opcode)
    b = recv_exactly(sock, 1)[0]
    return b, remaining - 1


def _read_u8_list(
    sock: socket.socket, remaining: int, opcode: int
) -> tuple[List[int], int]:
    """Formato: [u8 n] + n * [u8]"""
    n, remaining = _read_u8_with_remaining(sock, remaining, opcode)
    items = []
    for _ in range(n):
        v, remaining = _read_u8_with_remaining(sock, remaining, opcode)
        items.append(v)
    return items, remaining


def _read_u8_u8_dict(
    sock: socket.socket, remaining: int, opcode: int
) -> tuple[Dict[int, int], int]:
    """Formato: [u8 n] + n * ([u8 key][u8 val])"""
    n, remaining = _read_u8_with_remaining(sock, remaining, opcode)
    out: Dict[int, int] = {}
    for _ in range(n):
        k, remaining = _read_u8_with_remaining(sock, remaining, opcode)
        v, remaining = _read_u8_with_remaining(sock, remaining, opcode)
        out[k] = v
    return out, remaining


def _instantiate_message_for_opcode(opcode: int):
    """Crea una instancia del mensaje adecuado para el opcode dado (inner)."""
    if opcode == Opcodes.FINISHED:
        return Finished()
    elif opcode == Opcodes.NEW_MENU_ITEMS:
        return NewMenuItems()
    elif opcode == Opcodes.NEW_STORES:
        return NewStores()
    elif opcode == Opcodes.NEW_TRANSACTION_ITEMS:
        return NewTransactionItems()
    elif opcode == Opcodes.NEW_TRANSACTION:
        return NewTransactions()
    elif opcode == Opcodes.NEW_USERS:
        return NewUsers()
    else:
        raise ProtocolError(f"invalid embedded opcode: {opcode}", Opcodes.DATA_BATCH)


class DataBatch:
    """
    [opcode=DATA_BATCH][i32 length]
      [u8 list: table_ids]
      [u8 list: query_ids]
      [u16 reserved]              # flags / reservado
      [dict<u8,u8> meta]
      [u16 total_shards]
      [u16 shard_num]
      [embedded message]          # [u8 inner_opcode][i32 inner_len][inner_body]
    """

    def _initialize_fields(
        self,
        table_ids: Optional[List[int]],
        query_ids: Optional[List[int]], 
        meta: Optional[Dict[int, int]],
        total_shards: Optional[int],
        shard_num: Optional[int],
        reserved_u16: int,
        batch_bytes: Optional[bytes]
    ):
        """Inicializa todos los campos del DataBatch con valores por defecto o los proporcionados."""
        self.opcode = Opcodes.DATA_BATCH

        # Si los campos vienen None, asumimos que se usará para DESERIALIZAR y se inicializan
        # con valores neutros; luego read_from los completará.
        self.table_ids: List[int] = [] if table_ids is None else list(table_ids)
        self.query_ids: List[int] = [] if query_ids is None else list(query_ids)
        self.reserved_u16: int = int(reserved_u16)
        self.meta: Dict[int, int] = {} if meta is None else dict(meta)
        self.total_shards: int = 0 if total_shards is None else int(total_shards)
        self.shard_num: int = 0 if shard_num is None else int(shard_num)

        # Contenido embebido:
        self.batch_msg = None  # instancia parseada (solo deserialización)
        self.batch_bytes: Optional[bytes] = batch_bytes  # framing crudo (serialización)

    def _validate_u8_list(self, items: List[int], field_name: str):
        """Valida que una lista contenga solo valores u8 válidos."""
        if len(items) > 255:
            raise ValueError(f"{field_name} must have at most 255 items")
        if any(not 0 <= x <= 255 for x in items):
            raise ValueError(f"{field_name} must contain only u8 values (0-255)")

    def _validate_u8_dict(self, meta_dict: Dict[int, int]):
        """Valida que un diccionario tenga claves y valores u8 válidos."""
        if len(meta_dict) > 255:
            raise ValueError("meta size must be <= 255")
        for k, v in meta_dict.items():
            if not (0 <= int(k) <= 255 and 0 <= int(v) <= 255):
                raise ValueError("meta keys/values must be u8")

    def _validate_u16_field(self, value: int, field_name: str):
        """Valida que un valor esté en el rango u16 válido."""
        if not (0 <= value <= 0xFFFF):
            raise ValueError(f"{field_name} must be u16 (0-65535)")

    def _validate_serialization_parameters(
        self,
        table_ids: Optional[List[int]],
        query_ids: Optional[List[int]],
        meta: Optional[Dict[int, int]]
    ):
        """Valida los parámetros solo si el objeto parece destinado a serialización."""
        # Validaciones solo si el objeto parece destinado a serialización
        if table_ids is not None:
            self._validate_u8_list(self.table_ids, "table_ids")
        
        if query_ids is not None:
            self._validate_u8_list(self.query_ids, "query_ids")
        
        if meta is not None:
            self._validate_u8_dict(self.meta)
        
        # Validar campos u16
        self._validate_u16_field(self.reserved_u16, "reserved_u16")
        self._validate_u16_field(self.total_shards, "total_shards")
        self._validate_u16_field(self.shard_num, "shard_num")

    def __init__(
        self,
        *,
        # Parámetros para SERIALIZACIÓN (todos los previos al batch interno):
        table_ids: Optional[List[int]] = None,
        query_ids: Optional[List[int]] = None,
        meta: Optional[Dict[int, int]] = None,
        total_shards: Optional[int] = None,
        shard_num: Optional[int] = None,
        reserved_u16: int = 0,
        # Bytes ya enmarcados del mensaje embebido (opcional; se requiere para write_to)
        batch_bytes: Optional[bytes] = None,
    ):
        # Inicializar todos los campos
        self._initialize_fields(
            table_ids, query_ids, meta, total_shards, 
            shard_num, reserved_u16, batch_bytes
        )

        # Validar parámetros de serialización
        self._validate_serialization_parameters(table_ids, query_ids, meta)

    # --- Helper para framing del mensaje interno (opcional) ---
    @staticmethod
    def make_embedded(inner_opcode: int, inner_body: bytes) -> bytes:
        """
        Arma [u8 opcode][i32 length][body] a partir de un opcode y el cuerpo sin enmarcar.
        """
        if not (0 <= inner_opcode <= 255):
            raise ValueError("inner_opcode must be u8")
        if not isinstance(inner_body, (bytes, bytearray)):
            raise TypeError("inner_body must be bytes")
        framed = bytearray()
        framed.append(int(inner_opcode))
        framed.extend(len(inner_body).to_bytes(4, "little", signed=True))
        framed.extend(inner_body)
        return bytes(framed)

    def _read_data_batch_header(self, sock: socket.socket, remaining: int) -> int:
        """Lee el header del DataBatch: table_ids, query_ids, reserved, batch_number, meta, shards."""
        self.table_ids, remaining = _read_u8_list(sock, remaining, self.opcode)
        self.query_ids, remaining = _read_u8_list(sock, remaining, self.opcode)
        self.reserved_u16, remaining = read_u16(sock, remaining, self.opcode)
        self.batch_number, remaining = read_i64(sock, remaining, self.opcode)
        self.meta, remaining = _read_u8_u8_dict(sock, remaining, self.opcode)
        self.total_shards, remaining = read_u16(sock, remaining, self.opcode)
        self.shard_num, remaining = read_u16(sock, remaining, self.opcode)
        return remaining

    def _read_embedded_message(self, sock: socket.socket, remaining: int) -> int:
        """Lee el mensaje embebido dentro del DataBatch."""
        # Leer opcode y longitud del mensaje interno
        inner_opcode, remaining = _read_u8_with_remaining(sock, remaining, self.opcode)
        inner_len, remaining = read_i32(sock, remaining, self.opcode)

        # Crear e instanciar el mensaje apropiado
        inner_msg = _instantiate_message_for_opcode(inner_opcode)
        inner_msg.read_from(sock, inner_len)  # esta llamada consume exactamente inner_len o lanza
        self.batch_msg = inner_msg
        
        # Actualizar remaining después de leer el mensaje embebido
        remaining -= inner_len
        return remaining

    def _validate_remaining_bytes(self, remaining: int):
        """Valida que no queden bytes sin procesar después de la deserialización."""
        if remaining != 0:
            raise ProtocolError(
                "Indicated length doesn't match body length", self.opcode
            )

    def _handle_protocol_error(self, sock: socket.socket, remaining: int):
        """Maneja errores de protocolo consumiendo bytes restantes."""
        if remaining > 0:
            _ = recv_exactly(sock, remaining)

    # --- Deserialización (igual que antes) ---
    def read_from(self, sock: socket.socket, length: int):
        remaining = length
        try:
            # Leer header del DataBatch
            remaining = self._read_data_batch_header(sock, remaining)

            # Leer mensaje embebido
            remaining = self._read_embedded_message(sock, remaining)

            # Validar que no queden bytes sin procesar
            self._validate_remaining_bytes(remaining)

        except ProtocolError:
            self._handle_protocol_error(sock, remaining)
            raise

    # Dejo esta por si la usamos para escribir al socket del cliente
    # --- Serialización ---
    def write_to(self, sock: socket.socket):
        """
        Serializa el DataBatch y lo escribe en `sock`.
        Requiere `self.batch_bytes` con el mensaje embebido ya enmarcado
        ([u8 opcode][i32 length][body]).
        """
        # Generar los bytes usando to_bytes() y enviarlos
        message_bytes = self.to_bytes()
        sock.sendall(message_bytes)

    def _validate_batch_bytes(self):
        """Valida que batch_bytes esté presente y tenga formato correcto."""
        if self.batch_bytes is None:
            raise ProtocolError(
                "missing embedded batch bytes for DataBatch serialization", self.opcode
            )

        # Validación ligera del framing embebido
        if len(self.batch_bytes) < 5:
            raise ProtocolError("invalid embedded framing (too short)", self.opcode)
        
        # Chequeo de consistencia del length interno
        inner_len = int.from_bytes(self.batch_bytes[1:5], "little", signed=True)
        if inner_len < 0 or inner_len != len(self.batch_bytes) - 5:
            raise ProtocolError(
                "invalid embedded framing (length mismatch)", self.opcode
            )

    def _serialize_u8_list(self, items: List[int]) -> bytearray:
        """Serializa una lista como [u8 count] + items."""
        result = bytearray()
        result.append(len(items))
        result.extend(bytes(items))
        return result

    def _serialize_u8_dict(self, meta_dict: Dict[int, int]) -> bytearray:
        """Serializa un diccionario como [u8 count] + key-value pairs."""
        result = bytearray()
        result.append(len(meta_dict))
        for k, v in meta_dict.items():
            result.append(int(k))
            result.append(int(v))
        return result

    def _serialize_data_batch_body(self) -> bytearray:
        """Serializa el cuerpo completo del DataBatch."""
        body = bytearray()

        # [u8 list: table_ids]
        body.extend(self._serialize_u8_list(self.table_ids))

        # [u8 list: query_ids]
        body.extend(self._serialize_u8_list(self.query_ids))

        # [u16 reserved]
        body.extend(int(self.reserved_u16).to_bytes(2, "little", signed=False))

        # [i64 batch_number]
        body.extend(int(self.batch_number).to_bytes(8, "little", signed=True))

        # [dict<u8,u8> meta]
        body.extend(self._serialize_u8_dict(self.meta))

        # [u16 total_shards][u16 shard_num]
        body.extend(int(self.total_shards).to_bytes(2, "little", signed=False))
        body.extend(int(self.shard_num).to_bytes(2, "little", signed=False))

        # [embedded message bytes]
        body.extend(self.batch_bytes)

        return body

    def _create_final_message(self, body: bytearray) -> bytes:
        """Crea el mensaje final con header [opcode][length][body]."""
        final_message = bytearray()
        final_message.append(int(self.opcode))  # opcode u8
        final_message.extend(len(body).to_bytes(4, "little", signed=True))  # length i32
        final_message.extend(body)  # body completo
        return bytes(final_message)

    def _log_serialization_details(self, body: bytearray, result_bytes: bytes):
        """Registra detalles de la serialización para debugging."""
        logging.debug(
            "action: data_batch_to_bytes | batch_number: %d | "
            "table_ids: %s | query_ids: %s | total_shards: %d | shard_num: %d | "
            "body_size: %d bytes | final_size: %d bytes",
            getattr(self, 'batch_number', 0), self.table_ids, self.query_ids,
            self.total_shards, self.shard_num, len(body), len(result_bytes)
        )

    def to_bytes(self) -> bytes:
        """
        Genera los bytes completos del DataBatch sin escribir a socket.
        Requiere `self.batch_bytes` con el mensaje embebido ya enmarcado.
        Retorna el mensaje completo serializado como bytes.
        """
        # Validar que tenemos los datos necesarios
        self._validate_batch_bytes()

        # Serializar el cuerpo del mensaje
        body = self._serialize_data_batch_body()

        # Crear el mensaje final con framing
        result_bytes = self._create_final_message(body)
        
        # Log de lo que estamos generando
        self._log_serialization_details(body, result_bytes)
        
        return result_bytes


# --- FILTROS DE COLUMNAS ---

def filter_menu_items_columns(rows: List[RawMenuItems]) -> List[dict]:
    """Filtra las columnas innecesarias de MenuItem: category, is_seasonal, available_from, available_to"""
    filtered_rows = []
    for row in rows:
        filtered_row = {
            "product_id": row.product_id,
            "name": row.name,
            "price": row.price
        }
        filtered_rows.append(filtered_row)
    return filtered_rows


def filter_stores_columns(rows: List[RawStore]) -> List[dict]:
    """Filtra las columnas innecesarias de Store: street, postal_code, city, state, latitude, longitude"""
    filtered_rows = []
    for row in rows:
        filtered_row = {
            "store_id": row.store_id,
            "store_name": row.store_name
        }
        filtered_rows.append(filtered_row)
    return filtered_rows


def filter_transaction_items_columns(rows: List[RawTransactionItem]) -> List[dict]:
    """Filtra las columnas innecesarias de TransactionItem: unit_price"""
    filtered_rows = []
    for row in rows:
        filtered_row = {
            "transaction_id": row.transaction_id,
            "item_id": row.item_id,
            "quantity": row.quantity,
            "subtotal": row.subtotal,
            "created_at": row.created_at
        }
        filtered_rows.append(filtered_row)
    return filtered_rows


def filter_transactions_columns(rows: List[RawTransaction]) -> List[dict]:
    """Filtra las columnas innecesarias de Transaction: payment_method_id, voucher_id, original_amount, discount_applied"""
    filtered_rows = []
    for row in rows:
        filtered_row = {
            "transaction_id": row.transaction_id,
            "store_id": row.store_id,
            "user_id": row.user_id,
            "final_amount": row.final_amount,
            "created_at": row.created_at
        }
        filtered_rows.append(filtered_row)
    return filtered_rows


def filter_users_columns(rows: List[RawUser]) -> List[dict]:
    """Filtra las columnas innecesarias de User: gender, registered_at"""
    filtered_rows = []
    for row in rows:
        filtered_row = {
            "user_id": row.user_id,
            "birthdate": row.birthdate
        }
        filtered_rows.append(filtered_row)
    return filtered_rows


def filter_vouchers_columns(rows) -> List[dict]:
    """Filtra todas las columnas de Voucher ya que no se usan en ninguna query"""
    # Por ahora retornamos lista vacía ya que estas columnas no se usan en queries actuales
    return []


# --- FUNCIONES DE SERIALIZACIÓN PARA BATCH_BYTES ---

def _serialize_header(n_rows: int, batch_number: int, batch_status: int) -> bytearray:
    """Serializa el header común de todos los mensajes filtrados."""
    body = bytearray()
    # [i32 nRows][i64 batchNumber][u8 status]
    body.extend(n_rows.to_bytes(4, "little", signed=True))
    body.extend(batch_number.to_bytes(8, "little", signed=True))  
    body.extend(batch_status.to_bytes(1, "little", signed=False))
    return body


def _serialize_key_value_pair(key: str, value: str) -> bytearray:
    """Serializa un par clave-valor como [string key][string value]."""
    pair_bytes = bytearray()
    # [string key]
    key_bytes = key.encode("utf-8")
    pair_bytes.extend(len(key_bytes).to_bytes(4, "little", signed=True))
    pair_bytes.extend(key_bytes)
    # [string value] 
    value_bytes = str(value).encode("utf-8")
    pair_bytes.extend(len(value_bytes).to_bytes(4, "little", signed=True))
    pair_bytes.extend(value_bytes)
    return pair_bytes


def _serialize_row(row: dict, required_keys: List[str]) -> bytearray:
    """Serializa una fila con las claves requeridas."""
    row_bytes = bytearray()
    # [i32 n_pairs]
    row_bytes.extend(len(required_keys).to_bytes(4, "little", signed=True))
    
    # Serializar cada par clave-valor
    for key in required_keys:
        value = row.get(key, "")
        row_bytes.extend(_serialize_key_value_pair(key, value))
    
    return row_bytes


def _serialize_filtered_data(filtered_rows: List[dict], batch_number: int, batch_status: int, required_keys: List[str], table_name: str) -> bytes:
    """Función genérica para serializar datos filtrados."""
    n_rows = len(filtered_rows)
    
    # Serializar header
    body = _serialize_header(n_rows, batch_number, batch_status)
    
    # Serializar cada fila filtrada
    for row in filtered_rows:
        body.extend(_serialize_row(row, required_keys))
    
    logging.debug(f"action: serialize_{table_name} | rows: {n_rows} | body_size: {len(body)} bytes")
    return bytes(body)


def serialize_filtered_menu_items(filtered_rows: List[dict], batch_number: int, batch_status: int) -> bytes:
    """Serializa los datos filtrados de menu items en formato NewMenuItems."""
    required_keys = ["product_id", "name", "price"]
    return _serialize_filtered_data(filtered_rows, batch_number, batch_status, required_keys, "menu_items")


def serialize_filtered_stores(filtered_rows: List[dict], batch_number: int, batch_status: int) -> bytes:
    """Serializa los datos filtrados de stores en formato NewStores.""" 
    required_keys = ["store_id", "store_name"]
    return _serialize_filtered_data(filtered_rows, batch_number, batch_status, required_keys, "stores")


def serialize_filtered_transaction_items(filtered_rows: List[dict], batch_number: int, batch_status: int) -> bytes:
    """Serializa los datos filtrados de transaction items en formato NewTransactionItems."""
    required_keys = ["transaction_id", "item_id", "quantity", "subtotal", "created_at"]
    return _serialize_filtered_data(filtered_rows, batch_number, batch_status, required_keys, "transaction_items")


def serialize_filtered_transactions(filtered_rows: List[dict], batch_number: int, batch_status: int) -> bytes:
    """Serializa los datos filtrados de transactions en formato NewTransactions."""
    required_keys = ["transaction_id", "store_id", "user_id", "final_amount", "created_at"]
    return _serialize_filtered_data(filtered_rows, batch_number, batch_status, required_keys, "transactions")


def serialize_filtered_users(filtered_rows: List[dict], batch_number: int, batch_status: int) -> bytes:
    """Serializa los datos filtrados de users en formato NewUsers."""
    required_keys = ["user_id", "birthdate"]
    return _serialize_filtered_data(filtered_rows, batch_number, batch_status, required_keys, "users")


# --- FUNCIÓN DE CREACIÓN DEL WRAPPER ---

def _get_filter_functions_mapping():
    """Retorna el mapeo de opcodes a funciones de filtrado."""
    return {
        Opcodes.NEW_MENU_ITEMS: filter_menu_items_columns,
        Opcodes.NEW_STORES: filter_stores_columns,
        Opcodes.NEW_TRANSACTION_ITEMS: filter_transaction_items_columns,
        Opcodes.NEW_TRANSACTION: filter_transactions_columns,
        Opcodes.NEW_USERS: filter_users_columns,
    }


def _get_query_mappings():
    """Retorna el mapeo de opcodes a query IDs específicos para cada tabla."""
    return {
        Opcodes.NEW_TRANSACTION: [1, 3, 4],       # Transaction: query 1, query 3, query 4
        Opcodes.NEW_TRANSACTION_ITEMS: [2],        # TransactionItem: query 2
        Opcodes.NEW_MENU_ITEMS: [2],               # MenuItem: query 2
        Opcodes.NEW_STORES: [3],                   # Store: query 3
        Opcodes.NEW_USERS: [4],                    # User: query 4
    }


def _get_serialize_functions_mapping():
    """Retorna el mapeo de opcodes a funciones de serialización."""
    return {
        Opcodes.NEW_MENU_ITEMS: serialize_filtered_menu_items,
        Opcodes.NEW_STORES: serialize_filtered_stores,
        Opcodes.NEW_TRANSACTION_ITEMS: serialize_filtered_transaction_items,
        Opcodes.NEW_TRANSACTION: serialize_filtered_transactions,
        Opcodes.NEW_USERS: serialize_filtered_users,
    }


def _get_filter_function_for_opcode(opcode: int):
    """Obtiene la función de filtrado para el opcode dado."""
    filter_functions = _get_filter_functions_mapping()
    filter_func = filter_functions.get(opcode)
    if filter_func is None:
        raise ValueError(f"No filter function defined for opcode {opcode}")
    return filter_func


def _get_query_ids_for_opcode(opcode: int):
    """Obtiene los query IDs para el opcode dado."""
    query_mappings = _get_query_mappings()
    return query_mappings.get(opcode, [1])  # Default a [1] si no se encuentra


def _get_serialize_function_for_opcode(opcode: int):
    """Obtiene la función de serialización para el opcode dado."""
    serialize_functions = _get_serialize_functions_mapping()
    serialize_func = serialize_functions.get(opcode)
    if serialize_func is None:
        raise ValueError(f"No serialize function defined for opcode {opcode}")
    return serialize_func


def _filter_and_serialize_data(original_msg):
    """Filtra los datos del mensaje original y los serializa."""
    # Obtener función de filtrado y aplicar filtro
    filter_func = _get_filter_function_for_opcode(original_msg.opcode)
    filtered_rows = filter_func(original_msg.rows)
    
    # Obtener función de serialización y serializar datos filtrados
    serialize_func = _get_serialize_function_for_opcode(original_msg.opcode)
    inner_body = serialize_func(filtered_rows, original_msg.batch_number, original_msg.batch_status)
    
    return filtered_rows, inner_body


def _log_data_batch_creation(original_msg, query_ids, filtered_rows, inner_body, batch_bytes):
    """Registra información detallada sobre la creación del data batch."""
    table_name = _get_table_name_for_opcode(original_msg.opcode)
    logging.info(
        "action: create_data_batch | table: %s | opcode: %d | query_ids: %s | "
        "original_rows: %d | filtered_rows: %d | batch_number: %d | "
        "inner_body_size: %d bytes | batch_bytes_size: %d bytes",
        table_name, original_msg.opcode, query_ids, 
        original_msg.amount, len(filtered_rows), original_msg.batch_number,
        len(inner_body), len(batch_bytes)
    )


def _log_filtered_sample(original_msg, filtered_rows):
    """Registra una muestra de los datos filtrados para debugging."""
    if filtered_rows:
        table_name = _get_table_name_for_opcode(original_msg.opcode)
        sample_row = filtered_rows[0]
        logging.debug(
            "action: filtered_sample | table: %s | batch_number: %d | "
            "sample_keys: %s | sample_row: %s",
            table_name, original_msg.batch_number, list(sample_row.keys()), sample_row
        )


def _create_data_batch_wrapper(original_msg, query_ids, batch_bytes):
    """Crea el wrapper DataBatch con todos los metadatos necesarios."""
    wrapper = DataBatch(
        table_ids=[1],  # Por ahora usamos 1 como table_id genérico
        query_ids=query_ids,  # Queries específicas según la tabla
        meta={},
        total_shards=0,
        shard_num=0,
        batch_bytes=batch_bytes  # ¡AQUÍ está el mensaje filtrado serializado!
    )
    
    # Asignar batch_number para que sea accesible
    wrapper.batch_number = original_msg.batch_number
    
    return wrapper


def _add_backward_compatibility_data(wrapper, original_msg, filtered_rows):
    """Añade el diccionario filtered_data para backward compatibility."""
    table_name = _get_table_name_for_opcode(original_msg.opcode)
    wrapper.filtered_data = {
        "table_name": table_name,
        "rows": filtered_rows,
        "original_row_count": original_msg.amount,
        "filtered_row_count": len(filtered_rows),
        "batch_number": original_msg.batch_number,
        "batch_status": original_msg.batch_status
    }


def _log_data_batch_ready(original_msg):
    """Registra que el data batch está listo para transmisión."""
    table_name = _get_table_name_for_opcode(original_msg.opcode)
    logging.info(
        "action: data_batch_ready | table: %s | batch_number: %d | "
        "ready_for_transmission: True | can_use_write_to: True",
        table_name, original_msg.batch_number
    )


def create_filtered_data_batch(original_msg) -> DataBatch:
    """
    Toma un mensaje original (NewMenuItems, NewStores, etc.) y crea un DataBatch wrapper
    con las columnas filtradas y metadatos apropiados.
    """
    # Obtener query IDs para esta tabla
    query_ids = _get_query_ids_for_opcode(original_msg.opcode)
    
    # Filtrar y serializar datos
    filtered_rows, inner_body = _filter_and_serialize_data(original_msg)
    
    # Crear el mensaje embebido completo [opcode][length][body]
    batch_bytes = DataBatch.make_embedded(
        inner_opcode=original_msg.opcode,
        inner_body=inner_body
    )
    
    # Log detallado de lo que estamos armando
    _log_data_batch_creation(original_msg, query_ids, filtered_rows, inner_body, batch_bytes)
    
    # Log de muestra de datos filtrados
    _log_filtered_sample(original_msg, filtered_rows)
    
    # Crear DataBatch wrapper completo
    wrapper = _create_data_batch_wrapper(original_msg, query_ids, batch_bytes)
    
    # Crear el diccionario para backward compatibility
    _add_backward_compatibility_data(wrapper, original_msg, filtered_rows)
    
    # Log final
    _log_data_batch_ready(original_msg)
    
    return wrapper


def _get_table_name_for_opcode(opcode: int) -> str:
    """Mapea opcodes a nombres de tabla para el logging/debugging"""
    table_names = {
        Opcodes.NEW_MENU_ITEMS: "menu_items",
        Opcodes.NEW_STORES: "stores",
        Opcodes.NEW_TRANSACTION_ITEMS: "transaction_items",
        Opcodes.NEW_TRANSACTION: "transactions",
        Opcodes.NEW_USERS: "users",
    }
    return table_names.get(opcode, f"unknown_opcode_{opcode}")
