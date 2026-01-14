__all__ = (
    "Cursor",
    "DescribeTopicPartitionsRequest",
    "TopicRequest",
)

from dataclasses import dataclass
from typing import BinaryIO, List, Optional, Self

from ..primitives import read_compact_array, read_compact_string, read_int32, read_int8
from ..tagged_fields import read_tagged_fields

from .abstract import AbstractRequest
from .header import RequestHeader


@dataclass(frozen=True)
class TopicRequest:
    name: str

    @classmethod
    def read(cls, stream: BinaryIO) -> Self:
        name = read_compact_string(stream)
        read_tagged_fields(stream)
        return cls(name)


@dataclass(frozen=True)
class Cursor:
    topic_name: str
    partition_index: int

    @classmethod
    def read(cls, stream: BinaryIO) -> Self:
        topic_name = read_compact_string(stream)
        partition_index = read_int32(stream)
        read_tagged_fields(stream)
        return cls(topic_name, partition_index)


@dataclass(frozen=True)
class DescribeTopicPartitionsRequest(AbstractRequest):
    topics: List[TopicRequest]
    response_partition_limit: int
    cursor: Optional[Cursor]

    @classmethod
    def read(cls, stream: BinaryIO, header: RequestHeader) -> Self:
        topics = read_compact_array(stream, TopicRequest.read)
        response_partition_limit = read_int32(stream)
        if read_int8(stream) < 0:
            cursor = None
        else:
            cursor = Cursor.read(stream)
        read_tagged_fields(stream)
        return cls(
            header=header,
            topics=topics,
            response_partition_limit=response_partition_limit,
            cursor=cursor,
        )
