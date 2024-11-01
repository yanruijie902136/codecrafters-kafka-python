import struct
import uuid
from typing import Any, BinaryIO, Callable

DecodeFunction = Callable[[BinaryIO], Any]
EncodeFunction = Callable[[Any], bytes]


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


def decode_varint(binary_stream: BinaryIO, signed=False):
    BASE = 128
    integer, multiplier = 0, 1
    while True:
        continuation, n = divmod(int.from_bytes(binary_stream.read(1)), BASE)
        integer += n * multiplier
        if not continuation:
            break
        multiplier *= BASE

    if signed and integer >= (1 << 31):
        integer -= 1 << 32
    return integer


def encode_varint(integer: int):
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


def decode_varlong(binary_stream: BinaryIO, signed=False):
    integer = decode_varint(binary_stream)
    if signed and integer >= (1 << 63):
        integer -= 1 << 64
    return integer


def encode_varlong(integer: int):
    if integer < 0:
        integer += 1 << 64
    return encode_varint(integer)


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
    n = decode_varint(binary_stream) - 1
    return binary_stream.read(n).decode()


def encode_compact_string(string: str):
    return encode_varint(len(string) + 1) + string.encode()


def decode_nullable_string(binary_stream: BinaryIO):
    n = decode_int16(binary_stream)
    return None if n < 0 else binary_stream.read(n).decode()


def encode_nullable_string(string: str | None):
    if string is None:
        return encode_int16(-1)
    return encode_int16(len(string)) + string.encode()


def decode_compact_nullable_string(binary_stream: BinaryIO):
    n = decode_varint(binary_stream) - 1
    return None if n < 0 else binary_stream.read(n).decode()


def encode_compact_nullable_string(string: str | None):
    if string is None:
        return encode_varint(0)
    return encode_varint(len(string) + 1) + string.encode()


def decode_bytes(binary_stream: BinaryIO):
    n = decode_int32(binary_stream)
    return binary_stream.read(n)


def encode_bytes(data: bytes):
    return encode_int32(len(data)) + data


def decode_compact_bytes(binary_stream: BinaryIO):
    n = decode_varint(binary_stream) - 1
    return binary_stream.read(n)


def encode_compact_bytes(data: bytes):
    return encode_varint(len(data) + 1) + data


def decode_nullable_bytes(binary_stream: BinaryIO):
    n = decode_int32(binary_stream)
    return None if n < 0 else binary_stream.read(n)


def encode_nullable_bytes(data: bytes | None):
    if data is None:
        return encode_int32(-1)
    return encode_int32(len(data)) + data


def decode_compact_nullable_bytes(binary_stream: BinaryIO):
    n = decode_varint(binary_stream) - 1
    return None if n < 0 else binary_stream.read(n)


def encode_compact_nullable_bytes(data: bytes | None):
    if data is None:
        return encode_varint(0)
    return encode_varint(len(data) + 1) + data


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
    n = decode_varint(binary_stream) - 1
    return None if n < 0 else [decode_function(binary_stream) for _ in range(n)]


def encode_compact_array(array: list | None, encode_function: EncodeFunction | None = None):
    if array is None:
        return encode_varint(0)
    encoded_instances = b"".join(
        instance.encode() if encode_function is None else encode_function(instance) for instance in array
    )
    return encode_varint(len(array) + 1) + encoded_instances


def decode_tagged_fields(binary_stream: BinaryIO):
    assert binary_stream.read(1) == b"\x00", "Unexpected tagged fields."


def encode_tagged_fields():
    return b"\x00"
