from __future__ import annotations

import dataclasses
import io
import uuid

from ..primitive_types import decode_compact_string, decode_tagged_fields, decode_uuid

from .record import Record


@dataclasses.dataclass
class TopicRecord(Record):
    name: str
    topic_id: uuid.UUID

    @classmethod
    def decode(cls, byte_stream: io.BufferedIOBase) -> TopicRecord:
        record = TopicRecord(
            name=decode_compact_string(byte_stream),
            topic_id=decode_uuid(byte_stream),
        )
        decode_tagged_fields(byte_stream)
        return record
