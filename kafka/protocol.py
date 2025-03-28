import enum
import itertools
import struct
import uuid
from typing import Any, BinaryIO, Callable


type DecodeFunction = Callable[[BinaryIO], Any]
type EncodeFunction = Callable[[Any], bytes]


@enum.unique
class ApiKey(enum.IntEnum):
    FETCH = 1
    API_VERSIONS = 18
    DESCRIBE_TOPIC_PARTITIONS = 75

    @classmethod
    def decode(cls, reader: BinaryIO):
        return ApiKey(decode_int16(reader))

    def encode(self):
        return encode_int16(self)


@enum.unique
class ErrorCode(enum.IntEnum):
    NONE = 0
    UNKNOWN_TOPIC_OR_PARTITION = 3
    UNSUPPORTED_VERSION = 35
    UNKNOWN_TOPIC_ID = 100

    @classmethod
    def decode(cls, reader: BinaryIO):
        return ErrorCode(decode_int16(reader))

    def encode(self):
        return encode_int16(self)


def decode_boolean(reader: BinaryIO):
    return bool.from_bytes(reader.read(1))


def encode_boolean(b: bool):
    return b.to_bytes(1)


def decode_int8(reader: BinaryIO):
    return int.from_bytes(reader.read(1), signed=True)


def encode_int8(n: int):
    return n.to_bytes(1, signed=True)


def decode_int16(reader: BinaryIO):
    return int.from_bytes(reader.read(2), signed=True)


def encode_int16(n: int):
    return n.to_bytes(2, signed=True)


def decode_int32(reader: BinaryIO):
    return int.from_bytes(reader.read(4), signed=True)


def encode_int32(n: int):
    return n.to_bytes(4, signed=True)


def decode_int64(reader: BinaryIO):
    return int.from_bytes(reader.read(8), signed=True)


def encode_int64(n: int):
    return n.to_bytes(8, signed=True)


def decode_uint32(reader: BinaryIO):
    return int.from_bytes(reader.read(4))


def encode_uint32(n: int):
    return n.to_bytes(4)


def decode_unsigned_varint(reader: BinaryIO):
    n = 0
    for shamt in itertools.count(start=0, step=7):
        c = ord(reader.read(1))
        n += (c & 0x7F) << shamt
        if not (c & 0x80):
            return n


def decode_varint(reader: BinaryIO):
    n = decode_unsigned_varint(reader)
    return -((n + 1) >> 1) if (n & 1) else (n >> 1)


def encode_unsigned_varint(n: int):
    encoding = b""
    while True:
        n, c = divmod(n, 0x80)
        if n > 0:
            c |= 0x80
        encoding += c.to_bytes(1)
        if n == 0:
            return encoding


def encode_varint(n: int):
    return encode_unsigned_varint((n << 1) ^ (n >> 31))


def decode_varlong(reader: BinaryIO):
    return decode_varint(reader)


def encode_varlong(n: int):
    return encode_unsigned_varint((n << 1) ^ (n >> 63))


def decode_uuid(reader: BinaryIO):
    return uuid.UUID(bytes=reader.read(16))


def encode_uuid(u: uuid.UUID):
    return u.bytes


def decode_float64(reader: BinaryIO):
    return struct.unpack("f", reader.read(8))[0]


def encode_float64(f: float):
    return struct.pack("f", f)


def decode_string(reader: BinaryIO):
    n = decode_int16(reader)
    return reader.read(n).decode()


def encode_string(s: str):
    return encode_int16(len(s)) + s.encode()


def decode_compact_string(reader: BinaryIO):
    n = decode_unsigned_varint(reader) - 1
    return reader.read(n).decode()


def encode_compact_string(s: str):
    return encode_unsigned_varint(len(s) + 1) + s.encode()


def decode_nullable_string(reader: BinaryIO):
    n = decode_int16(reader)
    return None if n < 0 else reader.read(n).decode()


def encode_nullable_string(s: str | None):
    if s is None:
        return encode_int16(-1)
    return encode_int16(len(s)) + s.encode()


def decode_compact_nullable_string(reader: BinaryIO):
    n = decode_unsigned_varint(reader) - 1
    return None if n < 0 else reader.read(n).decode()


def encode_compact_nullable_string(s: str | None):
    if s is None:
        return encode_unsigned_varint(0)
    return encode_unsigned_varint(len(s) + 1) + s.encode()


def decode_bytes(reader: BinaryIO):
    n = decode_int32(reader)
    return reader.read(n)


def encode_bytes(b: bytes):
    return encode_int32(len(b)) + b


def decode_compact_bytes(reader: BinaryIO):
    n = decode_unsigned_varint(reader) - 1
    return reader.read(n)


def encode_compact_bytes(b: bytes):
    return encode_unsigned_varint(len(b) + 1) + b


def decode_nullable_bytes(reader: BinaryIO):
    n = decode_int32(reader)
    return None if n < 0 else reader.read(n)


def encode_nullable_bytes(b: bytes | None):
    if b is None:
        return encode_int32(-1)
    return encode_int32(len(b)) + b


def decode_compact_nullable_bytes(reader: BinaryIO):
    n = decode_unsigned_varint(reader) - 1
    return None if n < 0 else reader.read(n)


def encode_compact_nullable_bytes(b: bytes | None):
    if b is None:
        return encode_unsigned_varint(0)
    return encode_unsigned_varint(len(b) + 1) + b


def decode_array(reader: BinaryIO, decode_function: DecodeFunction):
    n = decode_int32(reader)
    return None if n < 0 else [decode_function(reader) for _ in range(n)]


def encode_array(arr: list | None, encode_function: EncodeFunction | None = None):
    if arr is None:
        return encode_int32(-1)
    encoded_instances = b"".join(
        instance.encode() if encode_function is None else encode_function(instance) for instance in arr
    )
    return encode_int32(len(arr)) + encoded_instances


def decode_compact_array(reader: BinaryIO, decode_function: DecodeFunction):
    n = decode_unsigned_varint(reader) - 1
    return None if n < 0 else [decode_function(reader) for _ in range(n)]


def encode_compact_array(arr: list | None, encode_function: EncodeFunction | None = None):
    if arr is None:
        return encode_unsigned_varint(0)
    encoded_instances = b"".join(
        instance.encode() if encode_function is None else encode_function(instance) for instance in arr
    )
    return encode_unsigned_varint(len(arr) + 1) + encoded_instances


def decode_tagged_fields(reader: BinaryIO):
    assert reader.read(1) == b"\x00", "Unexpected tagged fields."


def encode_tagged_fields():
    return b"\x00"
