__all__ = "FeatureLevelRecord",

from dataclasses import dataclass
from typing import BinaryIO, Self

from ..primitives import read_compact_string, read_int16
from ..tagged_fields import read_tagged_fields

from .abstract import AbstractMetadataRecord


@dataclass(frozen=True)
class FeatureLevelRecord(AbstractMetadataRecord):
    name: str
    feature_level: int

    @classmethod
    def read(cls, stream: BinaryIO) -> Self:
        record = cls(
            name=read_compact_string(stream),
            feature_level=read_int16(stream),
        )
        read_tagged_fields(stream)
        return record
