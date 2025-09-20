import socket
import logging

class ProtocolError(Exception):
    """Represents a framing/validation error while parsing or writing messages.

    `opcode` optionally identifies the message context in which the error occurred.
    """

    def __init__(self, message, opcode=None):
        super().__init__(message)
        self.opcode = opcode


class Opcodes:
    """Numeric opcodes of the wire protocol (u8)."""

    NEW_BETS = 0
    BETS_RECV_SUCCESS = 1
    BETS_RECV_FAIL = 2
    FINISHED = 3

class NewBets:
    """Inbound NEW_BETS message.

    Body layout:
      [n_bets:i32 LE]
      n_bets × {
        [n_pairs:i32 LE == 6]
        6 × [key:string][value:string]  // UTF-8 with i32 length prefix
      }

    Validates required keys and collects bets as `RawBet` instances.
    """

    def __init__(self):
        self.bets: list[dict[str, str]] = []
        self.opcode: int = Opcodes.NEW_BETS
        self.required = (
            "AGENCIA",
            "NOMBRE",
            "APELLIDO",
            "DOCUMENTO",
            "NACIMIENTO",
            "NUMERO",
        )
        self.amount: int = 0

    def __read_pair(self, sock: socket.socket, remaining: int) -> tuple[str, str, int]:
        """Read a single <key, value> pair, both as protocol [string]."""
        (key, remaining) = read_string(sock, remaining, self.opcode)
        (value, remaining) = read_string(sock, remaining, self.opcode)
        return (key, value, remaining)

    def __read_bet(self, sock: socket.socket, remaining: int) -> int:
        """Read one bet map, validate required keys, and append it as a dict."""
        curr_bet: dict[str, str] = {}
        (n_pairs, remaining) = read_i32(sock, remaining, self.opcode)

        # YA NO validamos que n_pairs sea 6. Leemos los que vengan.
        # if n_pairs != 6:
        #     raise ProtocolError("invalid body", self.opcode)

        for _ in range(0, n_pairs):
            (k, v, remaining) = self.__read_pair(sock, remaining)
            curr_bet[k] = v

        # Mantenemos la validación de que las claves requeridas existan. ¡Esto es bueno!
        if any(k not in curr_bet for k in self.required):
            raise ProtocolError("invalid body: missing required keys", self.opcode)

        # En lugar de crear un RawBet, simplemente añadimos el diccionario.
        self.bets.append(curr_bet)

        return remaining

    def read_from(self, sock, length: int):
        """Parse the complete NEW_BETS body and enforce exact-length consumption.

        Reads the `n_bets` counter and then consumes each bet map. If, after
        parsing, `remaining != 0`, raises ProtocolError. On parse failure, drains
        the remaining bytes (to keep the stream synchronized) and re-raises.
        """
        remaining = length
        try:
            n_bets, remaining = read_i32(sock, remaining, self.opcode)
            self.amount = n_bets
            for _ in range(n_bets):
                remaining = self.__read_bet(sock, remaining)
            if remaining != 0:
                raise ProtocolError(
                    "indicated length doesn't match body length", self.opcode
                )
        except ProtocolError:
            if remaining > 0:
                _ = recv_exactly(sock, remaining)
            raise


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


def read_string(sock: socket.socket, remaining: int, opcode: int) -> (str, int):
    """Read a protocol [string]: i32 length (validated) + UTF-8 bytes.

    Ensures a strictly positive length and sufficient remaining payload.
    Returns the decoded string and the updated `remaining`.
    """
    (key_len, remaining) = read_i32(sock, remaining, opcode)
    if key_len <= 0:
        raise ProtocolError("invalid body", opcode)
    if remaining < key_len:
        raise ProtocolError("indicated length doesn't match body length", opcode)
    try:
        s = recv_exactly(sock, key_len).decode("utf-8")
    except UnicodeDecodeError as e:
        raise ProtocolError("invalid body", opcode) from e
    remaining -= key_len
    return (s, remaining)


def recv_msg(sock: socket.socket):
    """Read a single framed message and dispatch by opcode.

    Reads opcode (u8) and length (i32 LE), validates length, then dispatches
    to the appropriate message class. Raises ProtocolError on invalid opcode.
    """
    opcode = read_u8(sock)
    (length, _) = read_i32(sock, 4, -1)
    if length < 0:
        raise ProtocolError("invalid length")
    if opcode == Opcodes.NEW_BETS:
        msg = NewBets()
        msg.read_from(sock, length)
        return msg
    if opcode == Opcodes.FINISHED:
        msg = Finished()
        msg.read_from(sock, length)
        return msg
    raise ProtocolError(f"invalid opcode: {opcode}")


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