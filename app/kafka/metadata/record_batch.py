import dataclasses
import io
import typing

from ..protocol import (
    Readable,
    decode_array,
    decode_int16,
    decode_int32,
    decode_int64,
    decode_int8,
    decode_uint32,
)

from .record import Record, decode_record


@dataclasses.dataclass(frozen=True)
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
    def decode(cls, readable: Readable) -> typing.Self:
        base_offset = decode_int64(readable)
        batch_length = decode_int32(readable)
        new_readable = io.BytesIO(readable.read(batch_length))
        return cls(
            base_offset=base_offset,
            batch_length=batch_length,
            partition_leader_epoch=decode_int32(new_readable),
            magic=decode_int8(new_readable),
            crc=decode_uint32(new_readable),
            attributes=decode_int16(new_readable),
            last_offset_delta=decode_int32(new_readable),
            base_timestamp=decode_int64(new_readable),
            max_timestamp=decode_int64(new_readable),
            producer_id=decode_int64(new_readable),
            producer_epoch=decode_int16(new_readable),
            base_sequence=decode_int32(new_readable),
            records=decode_array(new_readable, decode_record),
        )


def read_record_batches(topic_name: str, partition_index: int) -> typing.Generator[RecordBatch, None, None]:
    with open(
        f"/tmp/kraft-combined-logs/{topic_name}-{partition_index}/00000000000000000000.log", mode="rb"
    ) as reader:
        while reader.peek():
            yield RecordBatch.decode(reader)
