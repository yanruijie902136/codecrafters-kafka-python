from __future__ import annotations

import dataclasses
import io

from ..protocol import decode_compact_string, decode_int16, decode_tagged_fields

from .record import Record


@dataclasses.dataclass
class FeatureLevelRecord(Record):
    name: str
    feature_level: int

    @classmethod
    def decode(cls, byte_stream: io.BufferedIOBase) -> FeatureLevelRecord:
        record = FeatureLevelRecord(
            name=decode_compact_string(byte_stream),
            feature_level=decode_int16(byte_stream),
        )
        decode_tagged_fields(byte_stream)
        return record
