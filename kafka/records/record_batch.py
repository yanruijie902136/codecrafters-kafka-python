from __future__ import annotations

import dataclasses
import io

from ..primitive_types import (
    decode_array,
    decode_int8,
    decode_int16,
    decode_int32,
    decode_int64,
    decode_uint32,
)

from .record import Record, decode_record


@dataclasses.dataclass
class RecordBatch:
    base_offset: int
    batch_length: int
    partition_leader_epoch: int
    magic: int
    crc: int
    attributes: int
    last_offset_delta: int
    base_timestamp: int
    max_timestamp: int
    producer_id: int
    producer_epoch: int
    base_sequence: int
    records: list[Record]

    @classmethod
    def decode(cls, byte_stream: io.BufferedIOBase) -> RecordBatch:
        return RecordBatch(
            base_offset=decode_int64(byte_stream),
            batch_length=decode_int32(byte_stream),
            partition_leader_epoch=decode_int32(byte_stream),
            magic=decode_int8(byte_stream),
            crc=decode_uint32(byte_stream),
            attributes=decode_int16(byte_stream),
            last_offset_delta=decode_int32(byte_stream),
            base_timestamp=decode_int64(byte_stream),
            max_timestamp=decode_int64(byte_stream),
            producer_id=decode_int64(byte_stream),
            producer_epoch=decode_int16(byte_stream),
            base_sequence=decode_int32(byte_stream),
            records=decode_array(byte_stream, decode_record),
        )
