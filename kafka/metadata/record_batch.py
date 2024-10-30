import dataclasses

from ..primitive_types import (
    BinaryStream,
    decode_array,
    decode_int8,
    decode_int16,
    decode_int32,
    decode_int64,
    decode_uint32,
    encode_array,
    encode_int8,
    encode_int16,
    encode_int32,
    encode_int64,
    encode_uint32,
)

from .record import Record


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
    def decode(cls, binary_stream: BinaryStream):
        return RecordBatch(
            base_offset=decode_int64(binary_stream),
            batch_length=decode_int32(binary_stream),
            partition_leader_epoch=decode_int32(binary_stream),
            magic=decode_int8(binary_stream),
            crc=decode_uint32(binary_stream),
            attributes=decode_int16(binary_stream),
            last_offset_delta=decode_int32(binary_stream),
            base_timestamp=decode_int64(binary_stream),
            max_timestamp=decode_int64(binary_stream),
            producer_id=decode_int64(binary_stream),
            producer_epoch=decode_int16(binary_stream),
            base_sequence=decode_int32(binary_stream),
            records=decode_array(binary_stream, Record.decode),
        )

    def encode(self):
        return b"".join([
            encode_int64(self.base_offset),
            encode_int32(self.batch_length),
            encode_int32(self.partition_leader_epoch),
            encode_int8(self.magic),
            encode_uint32(self.crc),
            encode_int16(self.attributes),
            encode_int32(self.last_offset_delta),
            encode_int64(self.base_timestamp),
            encode_int64(self.max_timestamp),
            encode_int64(self.producer_id),
            encode_int16(self.producer_epoch),
            encode_int32(self.base_sequence),
            encode_array(self.records),
        ])
