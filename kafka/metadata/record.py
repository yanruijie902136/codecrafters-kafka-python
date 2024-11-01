import dataclasses
from typing import BinaryIO

from ..primitive_types import (
    decode_compact_array,
    decode_compact_bytes,
    decode_int8,
    decode_varint,
    decode_varlong,
    encode_compact_array,
    encode_compact_bytes,
    encode_int8,
    encode_varint,
    encode_varlong,
)

from .record_header import RecordHeader


@dataclasses.dataclass
class Record:
    length: int
    attributes: int
    timestamp_delta: int
    offset_delta: int
    key: bytes
    value_length: int
    value: bytes
    headers: list[RecordHeader]

    @classmethod
    def decode(cls, binary_stream: BinaryIO):
        record_kwargs = {
            "length": decode_varint(binary_stream),
            "attributes": decode_int8(binary_stream),
            "timestamp_delta": decode_varlong(binary_stream),
            "offset_delta": decode_varint(binary_stream),
            "key": decode_compact_bytes(binary_stream),
            "value_length": decode_varint(binary_stream),
        }
        # XXX: For some reason value_length is twice the actual length of value.
        record_kwargs["value"] = binary_stream.read(record_kwargs["value_length"] // 2)
        record_kwargs["headers"] = decode_compact_array(binary_stream, RecordHeader.decode)
        return Record(**record_kwargs)

    def encode(self):
        return b"".join([
            encode_varint(self.length),
            encode_int8(self.attributes),
            encode_varlong(self.timestamp_delta),
            encode_varint(self.offset_delta),
            encode_compact_bytes(self.key),
            encode_varint(self.value_length),
            self.value,
            encode_compact_array(self.headers),
        ])
