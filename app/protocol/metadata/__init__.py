__all__ = (
    "AbstractMetadataRecord",
    "FeatureLevelRecord",
    "PartitionRecord",
    "TopicRecord",
    "read_metadata_record",
)

from typing import BinaryIO

from ..primitives import read_int8

from .abstract import AbstractMetadataRecord
from .feature_level import FeatureLevelRecord
from .partition import PartitionRecord
from .topic import TopicRecord


def read_metadata_record(stream: BinaryIO) -> AbstractMetadataRecord:
    frame_version = read_int8(stream)
    record_type = read_int8(stream)
    version = read_int8(stream)

    match record_type:
        case 2:
            return TopicRecord.read(stream)
        case 3:
            return PartitionRecord.read(stream)
        case 12:
            return FeatureLevelRecord.read(stream)
        case _:
            raise ValueError(f"Unknown metadata record type: {record_type}")
