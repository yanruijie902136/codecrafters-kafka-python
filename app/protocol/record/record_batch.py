__all__ = "RecordBatch",

from dataclasses import dataclass
from typing import BinaryIO, List, Self

from ..primitives import read_int16, read_int32, read_int64, read_int8, read_uint32

from .record import Record


@dataclass(frozen=True)
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
    records: List[Record]

    @classmethod
    def read(cls, stream: BinaryIO) -> Self:
        base_offset = read_int64(stream)
        batch_length = read_int32(stream)
        partition_leader_epoch = read_int32(stream)
        magic = read_int8(stream)
        crc = read_uint32(stream)
        attributes = read_int16(stream)
        last_offset_delta = read_int32(stream)
        base_timestamp = read_int64(stream)
        max_timestamp = read_int64(stream)
        producer_id = read_int64(stream)
        producer_epoch = read_int16(stream)
        base_sequence = read_int32(stream)
        records_count = read_int32(stream)
        if records_count < 0:
            raise ValueError("Number of records cannot be negative")
        records = [Record.read(stream) for _ in range(records_count)]
        return cls(
            base_offset=base_offset,
            batch_length=batch_length,
            partition_leader_epoch=partition_leader_epoch,
            magic=magic,
            crc=crc,
            attributes=attributes,
            last_offset_delta=last_offset_delta,
            base_timestamp=base_timestamp,
            max_timestamp=max_timestamp,
            producer_id=producer_id,
            producer_epoch=producer_epoch,
            base_sequence=base_sequence,
            records=records,
        )
