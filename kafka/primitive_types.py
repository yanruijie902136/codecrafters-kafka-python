import itertools
import struct
import uuid
from typing import Any, BinaryIO, Callable


type DecodeFunction = Callable[[BinaryIO], Any]
type EncodeFunction = Callable[[Any], bytes]


def decode_boolean(binary_stream: BinaryIO):
    return bool.from_bytes(binary_stream.read(1))


def encode_boolean(boolean: bool):
    return boolean.to_bytes(1)


def decode_int8(binary_stream: BinaryIO):
    return int.from_bytes(binary_stream.read(1), signed=True)


def encode_int8(integer: int):
    return integer.to_bytes(1, signed=True)


def decode_int16(binary_stream: BinaryIO):
    return int.from_bytes(binary_stream.read(2), signed=True)


def encode_int16(integer: int):
    return integer.to_bytes(2, signed=True)


def decode_int32(binary_stream: BinaryIO):
    return int.from_bytes(binary_stream.read(4), signed=True)


def encode_int32(integer: int):
    return integer.to_bytes(4, signed=True)


def decode_int64(binary_stream: BinaryIO):
    return int.from_bytes(binary_stream.read(8), signed=True)


def encode_int64(integer: int):
    return integer.to_bytes(8, signed=True)


def decode_uint32(binary_stream: BinaryIO):
    return int.from_bytes(binary_stream.read(4))


def encode_uint32(integer: int):
    return integer.to_bytes(4)


def decode_unsigned_varint(binary_stream: BinaryIO):
    n = 0
    for shamt in itertools.count(start=0, step=7):
        c = ord(binary_stream.read(1))
        n += (c & 0x7F) << shamt
        if not (c & 0x80):
            return n


def decode_varint(binary_stream: BinaryIO):
    n = decode_unsigned_varint(binary_stream)
    return -((n + 1) >> 1) if (n & 1) else (n >> 1)


def encode_unsigned_varint(integer: int):
    encoding = b""
    while True:
        integer, c = divmod(integer, 0x80)
        if integer > 0:
            c |= 0x80
        encoding += c.to_bytes(1)
        if integer == 0:
            return encoding


def encode_varint(integer: int):
    return encode_unsigned_varint((integer << 1) ^ (integer >> 31))


def decode_varlong(binary_stream: BinaryIO):
    return decode_varint(binary_stream)


def encode_varlong(integer: int):
    return encode_unsigned_varint((integer << 1) ^ (integer >> 63))


def decode_uuid(binary_stream: BinaryIO):
    return uuid.UUID(bytes=binary_stream.read(16))


def encode_uuid(u: uuid.UUID):
    return u.bytes


def decode_float64(binary_stream: BinaryIO):
    return struct.unpack("f", binary_stream.read(8))[0]


def encode_float64(number: float):
    return struct.pack("f", number)


def decode_string(binary_stream: BinaryIO):
    n = decode_int16(binary_stream)
    return binary_stream.read(n).decode()


def encode_string(string: str):
    return encode_int16(len(string)) + string.encode()


def decode_compact_string(binary_stream: BinaryIO):
    n = decode_unsigned_varint(binary_stream) - 1
    return binary_stream.read(n).decode()


def encode_compact_string(string: str):
    return encode_unsigned_varint(len(string) + 1) + string.encode()


def decode_nullable_string(binary_stream: BinaryIO):
    n = decode_int16(binary_stream)
    return None if n < 0 else binary_stream.read(n).decode()


def encode_nullable_string(string: str | None):
    if string is None:
        return encode_int16(-1)
    return encode_int16(len(string)) + string.encode()


def decode_compact_nullable_string(binary_stream: BinaryIO):
    n = decode_unsigned_varint(binary_stream) - 1
    return None if n < 0 else binary_stream.read(n).decode()


def encode_compact_nullable_string(string: str | None):
    if string is None:
        return encode_unsigned_varint(0)
    return encode_unsigned_varint(len(string) + 1) + string.encode()


def decode_bytes(binary_stream: BinaryIO):
    n = decode_int32(binary_stream)
    return binary_stream.read(n)


def encode_bytes(data: bytes):
    return encode_int32(len(data)) + data


def decode_compact_bytes(binary_stream: BinaryIO):
    n = decode_unsigned_varint(binary_stream) - 1
    return binary_stream.read(n)


def encode_compact_bytes(data: bytes):
    return encode_unsigned_varint(len(data) + 1) + data


def decode_nullable_bytes(binary_stream: BinaryIO):
    n = decode_int32(binary_stream)
    return None if n < 0 else binary_stream.read(n)


def encode_nullable_bytes(data: bytes | None):
    if data is None:
        return encode_int32(-1)
    return encode_int32(len(data)) + data


def decode_compact_nullable_bytes(binary_stream: BinaryIO):
    n = decode_unsigned_varint(binary_stream) - 1
    return None if n < 0 else binary_stream.read(n)


def encode_compact_nullable_bytes(data: bytes | None):
    if data is None:
        return encode_unsigned_varint(0)
    return encode_unsigned_varint(len(data) + 1) + data


def decode_array(binary_stream: BinaryIO, decode_function: DecodeFunction):
    n = decode_int32(binary_stream)
    return None if n < 0 else [decode_function(binary_stream) for _ in range(n)]


def encode_array(array: list | None, encode_function: EncodeFunction | None = None):
    if array is None:
        return encode_int32(-1)
    encoded_instances = b"".join(
        instance.encode() if encode_function is None else encode_function(instance) for instance in array
    )
    return encode_int32(len(array)) + encoded_instances


def decode_compact_array(binary_stream: BinaryIO, decode_function: DecodeFunction):
    n = decode_unsigned_varint(binary_stream) - 1
    return None if n < 0 else [decode_function(binary_stream) for _ in range(n)]


def encode_compact_array(array: list | None, encode_function: EncodeFunction | None = None):
    if array is None:
        return encode_unsigned_varint(0)
    encoded_instances = b"".join(
        instance.encode() if encode_function is None else encode_function(instance) for instance in array
    )
    return encode_unsigned_varint(len(array) + 1) + encoded_instances


def decode_tagged_fields(binary_stream: BinaryIO):
    assert binary_stream.read(1) == b"\x00", "Unexpected tagged fields."


def encode_tagged_fields():
    return b"\x00"
