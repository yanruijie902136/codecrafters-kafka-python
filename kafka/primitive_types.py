import io
import typing
import uuid

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


def decode_int8(byte_stream: io.BytesIO):
    return int.from_bytes(byte_stream.read(1), signed=True)


def encode_int8(integer: int):
    return integer.to_bytes(1, signed=True)


def decode_int16(byte_stream: io.BytesIO):
    return int.from_bytes(byte_stream.read(2), signed=True)


def encode_int16(integer: int):
    return integer.to_bytes(2, signed=True)


def decode_int32(byte_stream: io.BytesIO):
    return int.from_bytes(byte_stream.read(4), signed=True)


def encode_int32(integer: int):
    return integer.to_bytes(4, signed=True)


def decode_int64(byte_stream: io.BytesIO):
    return int.from_bytes(byte_stream.read(8), signed=True)


def encode_int64(integer: int):
    return integer.to_bytes(8, signed=True)


def decode_varint(byte_stream: io.BytesIO):
    integer, multiplier = 0, 1
    while True:
        n = int.from_bytes(byte_stream.read(1), signed=False)
        integer += (n % 128) * multiplier
        if n < 128:
            return integer
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


def decode_uuid(byte_stream: io.BytesIO):
    return uuid.UUID(bytes=byte_stream.read(16))


def encode_uuid(uuid: uuid.UUID):
    return uuid.bytes


def decode_compact_string(byte_stream: io.BytesIO):
    n = decode_varint(byte_stream)
    n -= 1
    return byte_stream.read(n).decode()


def decode_nullable_string(byte_stream: io.BytesIO):
    n = decode_int16(byte_stream)
    return "" if n < 0 else byte_stream.read(n).decode()


DecodeFunction = typing.Callable[[io.BytesIO], typing.Any]


def decode_compact_array(byte_stream: io.BytesIO, decode_function: DecodeFunction):
    n = decode_varint(byte_stream)
    n -= 1
    return [decode_function(byte_stream) for _ in range(n)]


def encode_compact_array(array: list):
    return encode_varint(len(array) + 1) + b"".join(item.encode() for item in array)


def decode_tagged_fields(byte_stream: io.BytesIO):
    byte_stream.read(1)


def encode_tagged_fields():
    return b"\x00"
