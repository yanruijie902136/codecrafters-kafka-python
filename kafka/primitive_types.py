from typing import Any, Callable

__all__ = [
    "decode_int8", "encode_int8",
    "decode_int16", "encode_int16",
    "decode_int32", "encode_int32",
    "decode_int64", "encode_int64",
    "decode_varint", "encode_varint",
    "decode_uuid", "encode_uuid",
    "decode_compact_string",
    "decode_nullable_string",
    "decode_compact_array", "encode_compact_array",
    "decode_tagged_fields", "encode_tagged_fields",
]


def decode_int8(bytes: bytes):
    return int.from_bytes(bytes[:1], signed=True), bytes[1:]


def encode_int8(integer: int):
    return integer.to_bytes(1, signed=True)


def decode_int16(bytes: bytes):
    return int.from_bytes(bytes[:2], signed=True), bytes[2:]


def encode_int16(integer: int):
    return integer.to_bytes(2, signed=True)


def decode_int32(bytes: bytes):
    return int.from_bytes(bytes[:4], signed=True), bytes[4:]


def encode_int32(integer: int):
    return integer.to_bytes(4, signed=True)


def decode_int64(bytes: bytes):
    return int.from_bytes(bytes[:8], signed=True), bytes[8:]


def encode_int64(integer: int):
    return integer.to_bytes(8, signed=True)


def decode_varint(bytes: bytes):
    integer, multiplier = 0, 1
    while True:
        n, bytes = int.from_bytes(bytes[:1], signed=False), bytes[1:]
        integer += (n % 128) * multiplier
        if n < 128:
            return integer, bytes
        multiplier *= 128


def encode_varint(integer: int):
    encoding = b""
    while True:
        integer, n = divmod(integer, 128)
        if integer > 0:
            n += 128
        encoding += encode_int8(n)
        if integer == 0:
            return encoding


def decode_uuid(bytes: bytes):
    return bytes[:16], bytes[16:]


def encode_uuid(uuid: bytes):
    return uuid


def decode_compact_string(bytes: bytes):
    n, bytes = decode_varint(bytes)
    n -= 1
    return bytes[:n].decode(), bytes[n:]


def decode_nullable_string(bytes: bytes):
    n, bytes = decode_int16(bytes)
    if n < 0:
        return None, bytes
    return bytes[:n].decode(), bytes[n:]


def decode_compact_array(bytes: bytes, decode_func: Callable[[bytes], tuple[Any, bytes]]):
    n, bytes = decode_varint(bytes)
    n -= 1
    array = []
    while len(array) < n:
        item, bytes = decode_func(bytes)
        array.append(item)
    return array, bytes


def encode_compact_array(array: list):
    return encode_varint(len(array) + 1) + b"".join(item.encode() for item in array)


def decode_tagged_fields(bytes: bytes):
    return bytes[:1], bytes[1:]


def encode_tagged_fields():
    return b"\x00"
