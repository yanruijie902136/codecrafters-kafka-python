__all__ = "TopicRecord",

from dataclasses import dataclass
from typing import BinaryIO, Self
from uuid import UUID

from ..primitives import read_compact_string, read_uuid
from ..tagged_fields import read_tagged_fields

from .abstract import AbstractMetadataRecord


@dataclass(frozen=True)
class TopicRecord(AbstractMetadataRecord):
    name: str
    topic_id: UUID

    @classmethod
    def read(cls, stream: BinaryIO) -> Self:
        record = cls(
            name=read_compact_string(stream),
            topic_id=read_uuid(stream),
        )
        read_tagged_fields(stream)
        return record
