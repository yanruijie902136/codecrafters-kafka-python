import enum
import itertools
import typing
import uuid


class Readable(typing.Protocol):
    def read(self, n: int) -> bytes:
        ...


type DecodeFunction[T] = typing.Callable[[Readable], T]
type EncodeFunction[T] = typing.Callable[[T], bytes]


@enum.unique
class ApiKey(enum.IntEnum):
    FETCH = 1
    API_VERSIONS = 18
    DESCRIBE_TOPIC_PARTITIONS = 75

    @classmethod
    def decode(cls, readable: Readable) -> typing.Self:
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


def decode_uuid(readable: Readable) -> uuid.UUID:
    return uuid.UUID(bytes=readable.read(16))


def encode_uuid(u: uuid.UUID) -> bytes:
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
