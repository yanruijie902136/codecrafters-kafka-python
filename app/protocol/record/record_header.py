__all__ = "RecordHeader",

from dataclasses import dataclass
from typing import BinaryIO, Optional, Self

from ..primitives import read_varint
from ..utils import readexactly


@dataclass(frozen=True)
class RecordHeader:
    key: str
    value: Optional[bytes]

    @classmethod
    def read(cls, stream: BinaryIO) -> Self:
        header_key_length = read_varint(stream, signed=True)
        if header_key_length < 0:
            raise ValueError("Header key size cannot be negative")
        key = readexactly(stream, header_key_length).decode()
        header_value_length = read_varint(stream, signed=True)
        if header_value_length < 0:
            value = None
        else:
            value = readexactly(stream, header_value_length)
        return cls(key, value)
