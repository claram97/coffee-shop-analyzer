import socket


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
    def __init__(self, product_id: str, name: str, price: str, category: str, is_seasonal: str, available_from: str, available_to: str):
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
    def __init__(self, store_id: str, store_name: str, street: str, postal_code: str,
                 city: str, state: str, latitude: str, longitude: str):
        self.store_id = store_id
        self.store_name = store_name
        self.street = street
        self.postal_code = postal_code
        self.city = city
        self.state = state
        self.latitude = latitude
        self.longitude = longitude

class RawTransactionItem:
    def __init__(self, transaction_id: str, item_id: str, quantity: str,
                 unit_price: str, subtotal: str, created_at: str):
        self.transaction_id = transaction_id
        self.item_id = item_id
        self.quantity = quantity
        self.unit_price = unit_price
        self.subtotal = subtotal
        self.created_at = created_at

class RawTransaction:
    def __init__(self, transaction_id: str, store_id: str, payment_method_id: str,
                 voucher_id: str, user_id: str, original_amount: str,
                 discount_applied: str, final_amount: str, created_at: str):
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
    def __init__(self, voucher_id: str, voucher_code: str, discount_type: str,
                 discount_value: str, valid_from: str, valid_to: str):
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
            raise ProtocolError(f"Expected {len(self.required_keys)} pairs, got {n_pairs}", self.opcode)

        for _ in range(n_pairs):
            (key, remaining) = read_string(sock, remaining, self.opcode)
            (value, remaining) = read_string(sock, remaining, self.opcode)
            current_row_data[key] = value

        # Verificación de claves genérica
        missing_keys = [key for key in self.required_keys if key not in current_row_data]
        if missing_keys:
            received_keys = list(current_row_data.keys())
            raise ProtocolError(
                f"Missing required keys: {missing_keys}. Received keys: {received_keys}. Expected: {list(self.required_keys)}", 
                self.opcode
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
            kwargs = {mapping[k]: v for k, v in current_row_data.items() if k in mapping}
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
                raise ProtocolError("Indicated length doesn't match body length", self.opcode)
        except ProtocolError:
            if remaining > 0:
                _ = recv_exactly(sock, remaining)
            raise

# --- CLASES DE MENSAJES ESPECÍFICOS (Ahora mucho más simples) ---

class NewBets(TableMessage):
    """Mensaje NEW_BETS que ahora hereda de TableMessage."""
    def __init__(self):
        # Solo necesitamos definir la configuración específica de las apuestas
        required = (
            "NOMBRE", "APELLIDO", 
            "DOCUMENTO", "NACIMIENTO", "NUMERO"
        )
        # Le pasamos a la clase base el opcode, las claves y la clase que debe usar para crear cada fila
        super().__init__(
            opcode=Opcodes.NEW_BETS,
            required_keys=required,
            row_factory=RawBet  # Usará RawBet(**kwargs) para crear los objetos
        )

class NewMenuItems(TableMessage):
    """Mensaje NEW_PRODUCTS que ahora hereda de TableMessage."""

    def __init__(self):
        required = (
            "product_id", "name", "category", "price", "is_seasonal", 
            "available_from", "available_to"
        )
        super().__init__(
            opcode=Opcodes.NEW_MENU_ITEMS,
            required_keys=required,
            row_factory=RawMenuItems  # Usará RawMenuItems(**kwargs) para crear los objetos
        )

class NewTransactionItems(TableMessage):
    """Mensaje NEW_TRANSACTION_ITEMS que ahora hereda de TableMessage."""
    def __init__(self):
        required = (
            "transaction_id", "item_id", "quantity", 
            "unit_price", "subtotal", "created_at"
        )
        super().__init__(
            opcode=Opcodes.NEW_TRANSACTION_ITEMS,
            required_keys=required,
            row_factory=RawTransactionItem  # Usará RawTransactionItem(**kwargs) para crear los objetos
        )

class NewTransactions(TableMessage):
    """Mensaje NEW_TRANSACTIONS que ahora hereda de TableMessage."""
    def __init__(self):
        required = (
            "transaction_id", "store_id", "payment_method_id", 
            "voucher_id", "user_id", "original_amount", 
            "discount_applied", "final_amount", "created_at"
        )
        super().__init__(
            opcode=Opcodes.NEW_TRANSACTION,
            required_keys=required,
            row_factory=RawTransaction  # Usará RawTransaction(**kwargs) para crear los objetos
        )

class NewUsers(TableMessage):
    """Mensaje NEW_USERS que ahora hereda de TableMessage."""
    def __init__(self):
        required = (
            "user_id", "gender", "birthdate", "registered_at"
        )
        super().__init__(
            opcode=Opcodes.NEW_USERS,
            required_keys=required,
            row_factory=RawUser  # Usará RawUser(**kwargs) para crear los objetos
        )
class NewVouchers(TableMessage):
    """Mensaje NEW_VOUCHERS que ahora hereda de TableMessage."""
    def __init__(self):
        required = (
            "voucher_id", "voucher_code", "discount_type", 
            "discount_value", "valid_from", "valid_to"
        )
        super().__init__(
            opcode=Opcodes.NEW_VOUCHERS,
            required_keys=required,
            row_factory=RawVoucher  # Usará RawVoucher(**kwargs) para crear los objetos
        )

class NewPaymentMethods(TableMessage):
    """Mensaje NEW_PAYMENT_METHODS que ahora hereda de TableMessage."""
    def __init__(self):
        required = (
            "method_id", "method_name", "category"
        )
        super().__init__(
            opcode=Opcodes.NEW_PAYMENT_METHODS,
            required_keys=required,
            row_factory=RawPaymentMethod  # Usará RawPaymentMethod(**kwargs) para crear los objetos
        )

class NewStores(TableMessage):
    """Mensaje NEW_STORES que ahora hereda de TableMessage."""
    def __init__(self):
        required = (
            "store_id", "store_name", "street", 
            "postal_code", "city", "state", 
            "latitude", "longitude"
        )
        super().__init__(
            opcode=Opcodes.NEW_STORES,
            required_keys=required,
            row_factory=RawStore  # Usará RawStore(**kwargs) para crear los objetos
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


def read_i32(sock: socket.socket, remaining: int, opcode: int) -> tuple[int, int]:
    """Read a little-endian signed int32 and decrement `remaining` accordingly.

    Raises ProtocolError if fewer than 4 bytes remain to be read.
    """
    if remaining < 4:
        raise ProtocolError("indicated length doesn't match body length", opcode)
    remaining -= 4
    val = int.from_bytes(recv_exactly(sock, 4), byteorder="little", signed=True)
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
        return ("", remaining)  # Handle empty strings
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
        # La clase Finished no cambia
        msg = Finished()
    # ¡Añadimos el nuevo tipo de mensaje!
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
    else:
        raise ProtocolError(f"invalid opcode: {opcode}")

    if msg:
        msg.read_from(sock, length)
        return msg


def write_u8(sock, value: int) -> None:
    """Write a single unsigned byte (u8) using sendall()."""
    if not 0 <= value <= 255:
        raise ValueError("u8 out of range")
    sock.sendall(bytes([value]))


def write_i32(sock: socket.socket, value: int) -> None:
    """Write a little-endian signed int32 using sendall()."""
    sock.sendall(int(value).to_bytes(4, byteorder="little", signed=True))


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