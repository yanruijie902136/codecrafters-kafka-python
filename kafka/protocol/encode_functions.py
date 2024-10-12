import uuid
from typing import Any, Callable, Optional

__all__ = [
    "encode_boolean",
    "encode_int8",
    "encode_int16",
    "encode_int32",
    "encode_int64",
    "encode_varint",
    "encode_varlong",
    "encode_uuid",
    "encode_compact_string",
    "encode_compact_nullable_string",
    "encode_compact_array",
    "encode_tagged_fields",
]

EncodeFunction = Callable[[Any], bytes]


def encode_boolean(boolean: bool) -> bytes:
    return int(boolean).to_bytes(1)


def encode_int8(integer: int) -> bytes:
    return integer.to_bytes(1, signed=True)


def encode_int16(integer: int) -> bytes:
    return integer.to_bytes(2, signed=True)


def encode_int32(integer: int) -> bytes:
    return integer.to_bytes(4, signed=True)


def encode_int64(integer: int) -> bytes:
    return integer.to_bytes(8, signed=True)


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


def encode_varlong(integer: int) -> bytes:
    if integer < 0:
        integer += 1 << 64
    return encode_varint(integer)


def encode_uuid(uuid: uuid.UUID) -> bytes:
    return uuid.bytes


def encode_compact_string(string: str) -> bytes:
    return encode_varint(len(string) + 1) + string.encode()


def encode_compact_nullable_string(string: str) -> bytes:
    if not string:
        return encode_varint(0)
    return encode_varint(len(string) + 1) + string.encode()


def encode_compact_array(array: list, encode_function: Optional[EncodeFunction] = None) -> bytes:
    encoded_items = b"".join(
        item.encode() if encode_function is None else encode_function(item)
        for item in array
    )
    return encode_varint(len(array) + 1) + encoded_items


def encode_tagged_fields() -> bytes:
    return b"\x00"
