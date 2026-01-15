__all__ = (
    "PartitionProduceData",
    "ProduceRequest",
    "TopicProduceData",
)

from dataclasses import dataclass
from typing import BinaryIO, List, Optional, Self

from ..primitives import read_compact_array, read_compact_bytes, read_compact_string, read_int16, read_int32
from ..tagged_fields import read_tagged_fields

from .abstract import AbstractRequest
from .header import RequestHeader


@dataclass(frozen=True)
class PartitionProduceData:
    index: int
    records: Optional[bytes]

    @classmethod
    def read(cls, stream: BinaryIO) -> Self:
        index = read_int32(stream)
        records = read_compact_bytes(stream, allow_none=True)
        read_tagged_fields(stream)
        return cls(index, records)


@dataclass(frozen=True)
class TopicProduceData:
    name: str
    partition_data: List[PartitionProduceData]

    @classmethod
    def read(cls, stream: BinaryIO) -> Self:
        name = read_compact_string(stream)
        partition_data = read_compact_array(stream, PartitionProduceData.read)
        read_tagged_fields(stream)
        return cls(name, partition_data)


@dataclass(frozen=True)
class ProduceRequest(AbstractRequest):
    transactional_id: Optional[str]
    acks: int
    timeout_ms: int
    topic_data: List[TopicProduceData]

    @classmethod
    def read(cls, stream: BinaryIO, header: RequestHeader) -> Self:
        request = cls(
            header=header,
            transactional_id=read_compact_string(stream, allow_none=True),
            acks=read_int16(stream),
            timeout_ms=read_int32(stream),
            topic_data=read_compact_array(stream, TopicProduceData.read),
        )
        read_tagged_fields(stream)
        return request
