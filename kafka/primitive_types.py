import io
import uuid
from typing import Any, Callable, Optional

DecodeFunction = Callable[[io.BufferedIOBase], Any]
EncodeFunction = Callable[[Any], bytes]


def encode_boolean(boolean: bool) -> bytes:
    return int(boolean).to_bytes(1)


def decode_int8(byte_stream: io.BufferedIOBase) -> int:
    return int.from_bytes(byte_stream.read(1), signed=True)


def encode_int8(integer: int) -> bytes:
    return integer.to_bytes(1, signed=True)


def decode_int16(byte_stream: io.BufferedIOBase) -> int:
    return int.from_bytes(byte_stream.read(2), signed=True)


def encode_int16(integer: int) -> bytes:
    return integer.to_bytes(2, signed=True)


def decode_int32(byte_stream: io.BufferedIOBase) -> int:
    return int.from_bytes(byte_stream.read(4), signed=True)


def encode_int32(integer: int) -> bytes:
    return integer.to_bytes(4, signed=True)


def decode_int64(byte_stream: io.BufferedIOBase) -> int:
    return int.from_bytes(byte_stream.read(8), signed=True)


def encode_int64(integer: int) -> bytes:
    return integer.to_bytes(8, signed=True)


def decode_uint32(byte_stream: io.BufferedIOBase) -> int:
    return int.from_bytes(byte_stream.read(4))


def encode_uint32(integer: int) -> bytes:
    return integer.to_bytes(4, signed=False)


def decode_varint(byte_stream: io.BufferedIOBase, signed=False) -> int:
    BASE = 128
    integer, multiplier = 0, 1
    while True:
        continuation, n = divmod(int.from_bytes(byte_stream.read(1)), BASE)
        integer += n * multiplier
        if not continuation:
            break
        multiplier *= BASE

    if signed and integer >= (1 << 31):
        integer -= 1 << 32
    return integer


def encode_varint(integer: int) -> bytes:
    if integer < 0:
        integer += 1 << 32

    BASE = 128
    encoding = b""
    while True:
        integer, n = divmod(integer, BASE)
        if integer > 0:
            n += BASE
        encoding += n.to_bytes(1)
        if integer == 0:
            return encoding


def decode_varlong(byte_stream: io.BufferedIOBase, signed=False) -> int:
    integer = decode_varint(byte_stream)
    if signed and integer >= (1 << 63):
        integer -= 1 << 64
    return integer


def encode_varlong(integer: int) -> bytes:
    if integer < 0:
        integer += 1 << 64
    return encode_varint(integer)


def decode_uuid(byte_stream: io.BufferedIOBase) -> uuid.UUID:
    return uuid.UUID(bytes=byte_stream.read(16))


def encode_uuid(uuid: uuid.UUID) -> bytes:
    return uuid.bytes


def decode_compact_string(byte_stream: io.BufferedIOBase) -> str:
    n = decode_varint(byte_stream) - 1
    return byte_stream.read(n).decode()


def encode_compact_string(string: str) -> bytes:
    return encode_varint(len(string) + 1) + string.encode()


def decode_nullable_string(byte_stream: io.BufferedIOBase) -> str:
    n = decode_int16(byte_stream)
    return "" if n < 0 else byte_stream.read(n).decode()


def encode_compact_nullable_string(string: str) -> bytes:
    if not string:
        return encode_varint(0)
    return encode_varint(len(string) + 1) + string.encode()


def decode_compact_bytes(byte_stream: io.BufferedIOBase) -> bytes:
    n = decode_varint(byte_stream) - 1
    return byte_stream.read(n)


def decode_array(byte_stream: io.BufferedIOBase, decode_function: DecodeFunction) -> list:
    n = decode_int32(byte_stream)
    return [] if n < 0 else [decode_function(byte_stream) for _ in range(n)]


def decode_compact_array(byte_stream: io.BufferedIOBase, decode_function: DecodeFunction) -> list:
    n = decode_varint(byte_stream) - 1
    return [decode_function(byte_stream) for _ in range(n)]


def encode_compact_array(array: list, encode_function: Optional[EncodeFunction] = None) -> bytes:
    encoded_items = b"".join(
        item.encode() if encode_function is None else encode_function(item)
        for item in array
    )
    return encode_varint(len(array) + 1) + encoded_items


def decode_tagged_fields(byte_stream: io.BufferedIOBase) -> None:
    # There are no tagged fields in this challenge. TAG_BUFFER is always a null byte.
    assert byte_stream.read(1) == b"\x00", "Unexpected tagged fields."


def encode_tagged_fields() -> bytes:
    return b"\x00"
