__all__ = "PartitionRecord",

from dataclasses import dataclass
from typing import BinaryIO, List, Self
from uuid import UUID

from ..primitives import read_compact_array, read_int32, read_uuid
from ..tagged_fields import read_tagged_fields

from .abstract import AbstractMetadataRecord


@dataclass(frozen=True)
class PartitionRecord(AbstractMetadataRecord):
    partition_id: int
    topic_id: UUID
    replicas: List[int]
    isr: List[int]
    removing_replicas: List[int]
    adding_replicas: List[int]
    leader: int
    leader_epoch: int
    partition_epoch: int
    directories: List[UUID]

    @classmethod
    def read(cls, stream: BinaryIO) -> Self:
        record = cls(
            partition_id=read_int32(stream),
            topic_id=read_uuid(stream),
            replicas=read_compact_array(stream, read_int32),
            isr=read_compact_array(stream, read_int32),
            removing_replicas=read_compact_array(stream, read_int32),
            adding_replicas=read_compact_array(stream, read_int32),
            leader=read_int32(stream),
            leader_epoch=read_int32(stream),
            partition_epoch=read_int32(stream),
            directories=read_compact_array(stream, read_uuid),
        )
        read_tagged_fields(stream)
        return record
