from __future__ import annotations

import abc
import io

from ..protocol import (
    decode_compact_bytes,
    decode_int8,
    decode_varint,
    decode_varlong,
)


class Record(abc.ABC):
    @classmethod
    def decode(cls, byte_stream: io.BufferedIOBase) -> Record:
        raise NotImplementedError


def decode_record(byte_stream: io.BufferedIOBase) -> Record:
    decode_varint(byte_stream)
    decode_int8(byte_stream)
    decode_varlong(byte_stream)
    decode_varint(byte_stream)
    decode_compact_bytes(byte_stream)   # FIXME: Key length and key.
    decode_varint(byte_stream)
    decode_int8(byte_stream)
    record_type = decode_int8(byte_stream)
    decode_int8(byte_stream)

    match record_type:
        case 2:
            from .topic_record import TopicRecord
            record_class = TopicRecord
        case 3:
            from .partition_record import PartitionRecord
            record_class = PartitionRecord
        case 12:
            from .feature_level_record import FeatureLevelRecord
            record_class = FeatureLevelRecord
        case _:
            raise ValueError(f"Unsupported record type: {record_type}")

    record = record_class.decode(byte_stream)
    assert byte_stream.read(1) == b"\x00", "Unexpected header in record."
    return record
