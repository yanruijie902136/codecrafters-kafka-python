import io
import uuid
from typing import Any, Callable


DecodeFunction = Callable[[io.BytesIO], Any]


def decode_compact_array(byte_stream: io.BytesIO, decode_function: DecodeFunction) -> list:
    n = decode_varint(byte_stream) - 1
    return [decode_function(byte_stream) for _ in range(n)]


def decode_compact_string(byte_stream: io.BytesIO) -> str:
    n = decode_varint(byte_stream) - 1
    return byte_stream.read(n).decode()


def decode_int8(byte_stream: io.BytesIO) -> int:
    return int.from_bytes(byte_stream.read(1), signed=True)


def decode_int16(byte_stream: io.BytesIO) -> int:
    return int.from_bytes(byte_stream.read(2), signed=True)


def decode_int32(byte_stream: io.BytesIO) -> int:
    return int.from_bytes(byte_stream.read(4), signed=True)


def decode_int64(byte_stream: io.BytesIO) -> int:
    return int.from_bytes(byte_stream.read(8), signed=True)


def decode_nullable_string(byte_stream: io.BytesIO) -> str:
    n = decode_int16(byte_stream)
    return "" if n < 0 else byte_stream.read(n).decode()


def decode_tagged_fields(byte_stream: io.BytesIO) -> None:
    # There are no tagged fields in this challenge. TAG_BUFFER is always a null byte.
    assert byte_stream.read(1) == b"\x00", "Unexpected tagged fields."


def decode_uuid(byte_stream: io.BytesIO) -> uuid.UUID:
    return uuid.UUID(bytes=byte_stream.read(16))


def decode_varint(byte_stream: io.BytesIO) -> int:
    BASE = 128
    integer, multiplier = 0, 1
    while True:
        continuation, n = divmod(int.from_bytes(byte_stream.read(1), signed=False), BASE)
        integer += n * multiplier
        if not continuation:
            return integer
        multiplier *= BASE
