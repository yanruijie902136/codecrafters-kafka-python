import dataclasses
from typing import BinaryIO

from ..primitive_types import decode_varint, encode_varint


@dataclasses.dataclass
class RecordHeader:
    key: str
    value: bytes

    @classmethod
    def decode(cls, binary_stream: BinaryIO):
        key_length = decode_varint(binary_stream)
        key = binary_stream.read(key_length).decode()
        value_length = decode_varint(binary_stream)
        value = binary_stream.read(value_length)
        return RecordHeader(key, value)

    def encode(self):
        return b"".join([
            encode_varint(len(self.key)),
            self.key.encode(),
            encode_varint(len(self.value)),
            self.value,
        ])
