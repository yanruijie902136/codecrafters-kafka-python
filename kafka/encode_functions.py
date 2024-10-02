import uuid


def encode_compact_array(array: list) -> bytes:
    return encode_varint(len(array) + 1) + b"".join(item.encode() for item in array)


def encode_int8(integer: int) -> bytes:
    return integer.to_bytes(1, signed=True)


def encode_int16(integer: int) -> bytes:
    return integer.to_bytes(2, signed=True)


def encode_int32(integer: int) -> bytes:
    return integer.to_bytes(4, signed=True)


def encode_int64(integer: int) -> bytes:
    return integer.to_bytes(8, signed=True)


def encode_tagged_fields() -> bytes:
    return b"\x00"


def encode_uuid(uuid: uuid.UUID) -> bytes:
    return uuid.bytes


def encode_varint(integer: int) -> bytes:
    BASE = 128
    encoding = b""
    while True:
        integer, n = divmod(integer, BASE)
        if integer > 0:
            n += BASE
        encoding += encode_int8(n)
        if integer == 0:
            return encoding
