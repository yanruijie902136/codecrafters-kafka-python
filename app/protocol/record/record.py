__all__ = "Record",

from dataclasses import dataclass
from typing import BinaryIO, List, Optional, Self

from ..primitives import read_int8, read_varint, read_varlong
from ..utils import readexactly

from .record_header import RecordHeader


@dataclass(frozen=True)
class Record:
    length: int
    attributes: int
    timestamp_delta: int
    offset_delta: int
    key: Optional[bytes]
    value: Optional[bytes]
    headers: List[RecordHeader]

    @classmethod
    def read(cls, stream: BinaryIO) -> Self:
        length = read_varint(stream, signed=True)
        attributes = read_int8(stream)
        timestamp_delta = read_varlong(stream, signed=True)
        offset_delta = read_varlong(stream, signed=True)
        key_length = read_varint(stream, signed=True)
        if key_length < 0:
            key = None
        else:
            key = readexactly(stream, key_length)
        value_length = read_varint(stream, signed=True)
        if value_length < 0:
            value = None
        else:
            value = readexactly(stream, value_length)
        headers_count = read_varint(stream, signed=True)
        if headers_count < 0:
            raise ValueError("Number of record headers cannot be negative")
        headers = [RecordHeader.read(stream) for _ in range(headers_count)]
        return cls(
            length=length,
            attributes=attributes,
            timestamp_delta=timestamp_delta,
            offset_delta=offset_delta,
            key=key,
            value=value,
            headers=headers,
        )
