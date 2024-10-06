from __future__ import annotations

import dataclasses
import io
import uuid

from ..protocol import (
    decode_compact_array,
    decode_int32,
    decode_tagged_fields,
    decode_uuid,
)

from .record import Record


@dataclasses.dataclass
class PartitionRecord(Record):
    partition_id: int
    topic_id: uuid.UUID
    replicas: list[int]
    in_sync_replicas: list[int]
    removing_replicas: list[int]
    adding_replicas: list[int]
    leader: int
    leader_epoch: int
    partition_epoch: int
    directories: list[uuid.UUID]

    @classmethod
    def decode(cls, byte_stream: io.BufferedIOBase) -> PartitionRecord:
        record = PartitionRecord(
            partition_id=decode_int32(byte_stream),
            topic_id=decode_uuid(byte_stream),
            replicas=decode_compact_array(byte_stream, decode_int32),
            in_sync_replicas=decode_compact_array(byte_stream, decode_int32),
            removing_replicas=decode_compact_array(byte_stream, decode_int32),
            adding_replicas=decode_compact_array(byte_stream, decode_int32),
            leader=decode_int32(byte_stream),
            leader_epoch=decode_int32(byte_stream),
            partition_epoch=decode_int32(byte_stream),
            directories=decode_compact_array(byte_stream, decode_uuid),
        )
        decode_tagged_fields(byte_stream)
        return record
