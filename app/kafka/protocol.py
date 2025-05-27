import enum
import itertools
from typing import Callable, Self, Protocol
from uuid import UUID

__all__ = [
    "ApiKey",
    "ErrorCode",
    "Readable",
    "decode_array",
    "decode_compact_array",
    "decode_compact_string",
    "decode_int16",
    "decode_int32",
    "decode_int64",
    "decode_int8",
    "decode_nullable_string",
    "decode_uint32",
    "decode_unsigned_varint",
    "decode_uuid",
    "decode_tagged_fields",
    "decode_varint",
    "decode_varlong",
    "encode_array",
    "encode_boolean",
    "encode_compact_array",
    "encode_compact_nullable_string",
    "encode_compact_string",
    "encode_int16",
    "encode_int32",
    "encode_int64",
    "encode_int8",
    "encode_uint32",
    "encode_unsigned_varint",
    "encode_uuid",
    "encode_tagged_fields",
    "encode_varint",
    "encode_varlong",
]


class Readable(Protocol):
    def read(self, n: int, /) -> bytes:
        ...


type DecodeFunction[T] = Callable[[Readable], T]
type EncodeFunction[T] = Callable[[T], bytes]


@enum.unique
class ApiKey(enum.IntEnum):
    FETCH = 1
    API_VERSIONS = 18
    DESCRIBE_TOPIC_PARTITIONS = 75

    @classmethod
    def decode(cls, readable: Readable) -> Self:
        return cls(decode_int16(readable))

    def encode(self) -> bytes:
        return encode_int16(self)


@enum.unique
class ErrorCode(enum.IntEnum):
    NONE = 0
    UNKNOWN_TOPIC_OR_PARTITION = 3
    UNSUPPORTED_VERSION = 35
    UNKNOWN_TOPIC_ID = 100

    def encode(self) -> bytes:
        return encode_int16(self)


def encode_boolean(b: bool) -> bytes:
    return b.to_bytes(1)


def decode_int8(readable: Readable) -> int:
    return int.from_bytes(readable.read(1), signed=True)


def encode_int8(n: int) -> bytes:
    return n.to_bytes(1, signed=True)


def decode_int16(readable: Readable) -> int:
    return int.from_bytes(readable.read(2), signed=True)


def encode_int16(n: int) -> bytes:
    return n.to_bytes(2, signed=True)


def decode_int32(readable: Readable) -> int:
    return int.from_bytes(readable.read(4), signed=True)


def encode_int32(n: int) -> bytes:
    return n.to_bytes(4, signed=True)


def decode_int64(readable: Readable) -> int:
    return int.from_bytes(readable.read(8), signed=True)


def encode_int64(n: int) -> bytes:
    return n.to_bytes(8, signed=True)


def decode_uint32(readable: Readable) -> int:
    return int.from_bytes(readable.read(4))


def encode_uint32(n: int) -> bytes:
    return n.to_bytes(4)


def decode_unsigned_varint(readable: Readable) -> int:
    n = 0
    for shamt in itertools.count(start=0, step=7):
        c = ord(readable.read(1))
        n += (c & 0x7f) << shamt
        if not (c & 0x80):
            return n


def encode_unsigned_varint(n: int) -> bytes:
    encoding = b""
    while True:
        n, c = n >> 7, n & (0x7f)
        if n > 0:
            c |= 0x80
        encoding += c.to_bytes(1)
        if n == 0:
            return encoding


def decode_varint(readable: Readable) -> int:
    n = decode_unsigned_varint(readable)
    return -((n >> 1) + 1) if (n & 1) else (n >> 1)


def encode_varint(n: int) -> bytes:
    return encode_unsigned_varint((n << 1) ^ (n >> 31))


def decode_varlong(readable: Readable) -> int:
    return decode_varint(readable)


def encode_varlong(n: int) -> bytes:
    return encode_unsigned_varint((n << 1) ^ (n >> 63))


def decode_uuid(readable: Readable) -> UUID:
    return UUID(bytes=readable.read(16))


def encode_uuid(u: UUID) -> bytes:
    return u.bytes


def decode_compact_string(readable: Readable) -> str:
    n = decode_unsigned_varint(readable)
    assert n > 0, "incorrect compact string format"
    return readable.read(n - 1).decode()


def encode_compact_string(s: str) -> bytes:
    return encode_unsigned_varint(len(s) + 1) + s.encode()


def decode_nullable_string(readable: Readable) -> str | None:
    n = decode_int16(readable)
    return None if n < 0 else readable.read(n).decode()


def encode_compact_nullable_string(s: str | None) -> bytes:
    if s is None:
        return encode_unsigned_varint(0)
    return encode_unsigned_varint(len(s) + 1) + s.encode()


def decode_array[T](readable: Readable, decode_function: DecodeFunction[T]) -> list[T]:
    n = decode_int32(readable)
    return [] if n < 0 else [decode_function(readable) for _ in range(n)]


def encode_array[T](arr: list[T], encode_function: EncodeFunction[T] | None = None) -> bytes:
    return encode_int32(len(arr)) + b"".join(
        t.encode() if encode_function is None else encode_function(t) for t in arr
    )


def decode_compact_array[T](readable: Readable, decode_function: DecodeFunction[T]) -> list[T]:
    n = decode_unsigned_varint(readable)
    return [] if n == 0 else [decode_function(readable) for _ in range(n - 1)]


def encode_compact_array[T](arr: list[T], encode_function: EncodeFunction[T] | None = None) -> bytes:
    return encode_unsigned_varint(len(arr) + 1) + b"".join(
        t.encode() if encode_function is None else encode_function(t) for t in arr
    )


def decode_tagged_fields(readable: Readable) -> None:
    assert readable.read(1) == b"\x00", "incorrect tagged fields format"


def encode_tagged_fields() -> bytes:
    return b"\x00"
