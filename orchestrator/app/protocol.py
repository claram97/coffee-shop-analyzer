import socket
import struct
from typing import Optional, List, Dict


class ProtocolError(Exception):
    """Represents a framing/validation error while parsing or writing messages.

    `opcode` optionally identifies the message context in which the error occurred.
    """

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


class RawBet:
    """Transport-level bet structure read from the wire (not the domain model)."""

    def __init__(
        self,
        first_name: str,
        last_name: str,
        document: str,
        birthdate: str,
        number: str,
    ):
        self.first_name = first_name
        self.last_name = last_name
        self.document = document
        self.birthdate = birthdate
        self.number = number


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


class RawPaymentMethod:
    def __init__(self, method_id: str, method_name: str, category: str):
        self.method_id = method_id
        self.method_name = method_name
        self.category = category


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
        voucher_id: str,
        user_id: str,
        original_amount: str,
        discount_applied: str,
        final_amount: str,
        created_at: str,
    ):
        self.transaction_id = transaction_id
        self.store_id = store_id
        self.payment_method_id = payment_method_id
        self.voucher_id = voucher_id
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


class RawVoucher:
    def __init__(
        self,
        voucher_id: str,
        voucher_code: str,
        discount_type: str,
        discount_value: str,
        valid_from: str,
        valid_to: str,
    ):
        self.voucher_id = voucher_id
        self.voucher_code = voucher_code
        self.discount_type = discount_type
        self.discount_value = discount_value
        self.valid_from = valid_from
        self.valid_to = valid_to


class TableMessage:
    """
    Clase base genérica para leer un mensaje que contiene una tabla de datos.
    Una tabla es una lista de registros, donde cada registro es un mapa de
    clave-valor.
    """

    def __init__(self, opcode: int, required_keys: tuple[str, ...], row_factory):
        self.opcode = opcode
        self.required_keys = required_keys
        self.rows = []  # Almacenará los objetos creados (e.g., RawBet, RawProduct)
        self.amount = 0
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

        # Mapeo explícito para RawBet
        if self._row_factory == RawBet:
            mapping = {
                "NOMBRE": "first_name",
                "APELLIDO": "last_name",
                "DOCUMENTO": "document",
                "NACIMIENTO": "birthdate",
                "NUMERO": "number",
            }
            kwargs = {
                mapping[k]: v for k, v in current_row_data.items() if k in mapping
            }
            self.rows.append(self._row_factory(**kwargs))
        else:
            # Por defecto: claves a minúsculas
            kwargs = {key.lower(): value for key, value in current_row_data.items()}
            self.rows.append(self._row_factory(**kwargs))
        return remaining

    def read_from(self, sock: socket.socket, length: int):
        """Parsea el cuerpo completo del mensaje de la tabla."""
        remaining = length
        try:
            (n_rows, remaining) = read_i32(sock, remaining, self.opcode)
            self.amount = n_rows
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


class NewBets(TableMessage):
    """Mensaje NEW_BETS que ahora hereda de TableMessage."""

    def __init__(self):
        # Solo necesitamos definir la configuración específica de las apuestas
        required = ("NOMBRE", "APELLIDO", "DOCUMENTO", "NACIMIENTO", "NUMERO")
        # Le pasamos a la clase base el opcode, las claves y la clase que debe usar para crear cada fila
        super().__init__(
            opcode=Opcodes.NEW_BETS,
            required_keys=required,
            row_factory=RawBet,  # Usará RawBet(**kwargs) para crear los objetos
        )


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
            "voucher_id",
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


class NewVouchers(TableMessage):
    """Mensaje NEW_VOUCHERS que ahora hereda de TableMessage."""

    def __init__(self):
        required = (
            "voucher_id",
            "voucher_code",
            "discount_type",
            "discount_value",
            "valid_from",
            "valid_to",
        )
        super().__init__(
            opcode=Opcodes.NEW_VOUCHERS,
            required_keys=required,
            row_factory=RawVoucher,  # Usará RawVoucher(**kwargs) para crear los objetos
        )


class NewPaymentMethods(TableMessage):
    """Mensaje NEW_PAYMENT_METHODS que ahora hereda de TableMessage."""

    def __init__(self):
        required = ("method_id", "method_name", "category")
        super().__init__(
            opcode=Opcodes.NEW_PAYMENT_METHODS,
            required_keys=required,
            row_factory=RawPaymentMethod,  # Usará RawPaymentMethod(**kwargs) para crear los objetos
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
    if opcode == Opcodes.NEW_BETS:
        msg = NewBets()
    elif opcode == Opcodes.FINISHED:
        msg = Finished()
    elif opcode == Opcodes.NEW_MENU_ITEMS:
        msg = NewMenuItems()
    elif opcode == Opcodes.NEW_PAYMENT_METHODS:
        msg = NewPaymentMethods()
    elif opcode == Opcodes.NEW_STORES:
        msg = NewStores()
    elif opcode == Opcodes.NEW_TRANSACTION_ITEMS:
        msg = NewTransactionItems()
    elif opcode == Opcodes.NEW_TRANSACTION:
        msg = NewTransactions()
    elif opcode == Opcodes.NEW_USERS:
        msg = NewUsers()
    elif opcode == Opcodes.NEW_VOUCHERS:
        msg = NewVouchers()
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
    if opcode == Opcodes.NEW_BETS:
        return NewBets()
    elif opcode == Opcodes.FINISHED:
        return Finished()
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
    else:
        raise ProtocolError(f"invalid embedded opcode: {opcode}", Opcodes.DATA_BATCH)


class DataBatch:
    """
    [opcode=DATA_BATCH][i32 length]
      [u8 list: table_ids]
      [u8 list: query_ids]
      [u16 reserved]              # flags / reservado
      [i64 batch_number]
      [dict<u8,u8> meta]
      [u16 total_shards]
      [u16 shard_num]
      [embedded message]          # [u8 inner_opcode][i32 inner_len][inner_body]
    """

    def __init__(
        self,
        *,
        # Parámetros para SERIALIZACIÓN (todos los previos al batch interno):
        table_ids: Optional[List[int]] = None,
        query_ids: Optional[List[int]] = None,
        batch_number: Optional[int] = None,
        meta: Optional[Dict[int, int]] = None,
        total_shards: Optional[int] = None,
        shard_num: Optional[int] = None,
        reserved_u16: int = 0,
        # Bytes ya enmarcados del mensaje embebido (opcional; se requiere para write_to)
        batch_bytes: Optional[bytes] = None,
    ):
        self.opcode = Opcodes.DATA_BATCH

        # Si los campos vienen None, asumimos que se usará para DESERIALIZAR y se inicializan
        # con valores neutros; luego read_from los completará.
        self.table_ids: List[int] = [] if table_ids is None else list(table_ids)
        self.query_ids: List[int] = [] if query_ids is None else list(query_ids)
        self.reserved_u16: int = int(reserved_u16)
        self.batch_number: int = 0 if batch_number is None else int(batch_number)
        self.meta: Dict[int, int] = {} if meta is None else dict(meta)
        self.total_shards: int = 1 if total_shards is None else int(total_shards)
        self.shard_num: int = 0 if shard_num is None else int(shard_num)

        # Contenido embebido:
        self.batch_msg = None  # instancia parseada (solo deserialización)
        self.batch_bytes: Optional[bytes] = batch_bytes  # framing crudo (serialización)

        # Validaciones solo si el objeto parece destinado a serialización
        if table_ids is not None:
            if len(self.table_ids) > 255 or any(
                not 0 <= x <= 255 for x in self.table_ids
            ):
                raise ValueError("table_ids must be u8 and at most 255 items")
        if query_ids is not None:
            if len(self.query_ids) > 255 or any(
                not 0 <= x <= 255 for x in self.query_ids
            ):
                raise ValueError("query_ids must be u8 and at most 255 items")
        if meta is not None:
            if len(self.meta) > 255:
                raise ValueError("meta size must be <= 255")
            for k, v in self.meta.items():
                if not (0 <= int(k) <= 255 and 0 <= int(v) <= 255):
                    raise ValueError("meta keys/values must be u8")
        if not (0 <= self.reserved_u16 <= 0xFFFF):
            raise ValueError("reserved_u16 must be u16")
        if not (0 <= self.total_shards <= 0xFFFF):
            raise ValueError("total_shards must be u16")
        if not (0 <= self.shard_num <= 0xFFFF):
            raise ValueError("shard_num must be u16")

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

    # --- Deserialización (igual que antes) ---
    def read_from(self, sock: socket.socket, length: int):
        remaining = length
        try:
            self.table_ids, remaining = _read_u8_list(sock, remaining, self.opcode)
            self.query_ids, remaining = _read_u8_list(sock, remaining, self.opcode)
            self.reserved_u16, remaining = read_u16(sock, remaining, self.opcode)
            self.batch_number, remaining = read_i64(sock, remaining, self.opcode)
            self.meta, remaining = _read_u8_u8_dict(sock, remaining, self.opcode)
            self.total_shards, remaining = read_u16(sock, remaining, self.opcode)
            self.shard_num, remaining = read_u16(sock, remaining, self.opcode)

            inner_opcode, remaining = _read_u8_with_remaining(
                sock, remaining, self.opcode
            )
            inner_len, remaining = read_i32(sock, remaining, self.opcode)

            inner_msg = _instantiate_message_for_opcode(inner_opcode)
            inner_msg.read_from(
                sock, inner_len
            )  # esta llamada consume exactamente inner_len o lanza
            self.batch_msg = inner_msg
            remaining -= inner_len

            if remaining != 0:
                raise ProtocolError(
                    "Indicated length doesn't match body length", self.opcode
                )

        except ProtocolError:
            if remaining > 0:
                _ = recv_exactly(sock, remaining)
            raise

    # --- Serialización ---
    def write_to(self, sock: socket.socket):
        """
        Serializa el DataBatch y lo escribe en `sock`.
        Requiere `self.batch_bytes` con el mensaje embebido ya enmarcado
        ([u8 opcode][i32 length][body]).
        """
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

        body = bytearray()

        # [u8 list: table_ids]
        body.append(len(self.table_ids))
        body.extend(bytes(self.table_ids))

        # [u8 list: query_ids]
        body.append(len(self.query_ids))
        body.extend(bytes(self.query_ids))

        # [u16 reserved]
        body.extend(int(self.reserved_u16).to_bytes(2, "little", signed=False))

        # [i64 batch_number]
        body.extend(int(self.batch_number).to_bytes(8, "little", signed=True))

        # [dict<u8,u8> meta]
        body.append(len(self.meta))
        for k, v in self.meta.items():
            body.append(int(k))
            body.append(int(v))

        # [u16 total_shards][u16 shard_num]
        body.extend(int(self.total_shards).to_bytes(2, "little", signed=False))
        body.extend(int(self.shard_num).to_bytes(2, "little", signed=False))

        # [embedded message bytes]
        body.extend(self.batch_bytes)

        # Frame final: [opcode][i32 length][body]
        write_u8(sock, self.opcode)
        write_i32(sock, len(body))
        sock.sendall(body)
