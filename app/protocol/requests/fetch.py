__all__ = (
    "FetchPartition",
    "FetchRequest",
    "FetchTopic",
    "ForgottenTopic",
)

from dataclasses import dataclass
from typing import BinaryIO, List, Self
from uuid import UUID

from ..primitives import (
    read_compact_array,
    read_compact_string,
    read_int32,
    read_int64,
    read_int8,
    read_uuid,
)
from ..tagged_fields import read_tagged_fields

from .abstract import AbstractRequest
from .header import RequestHeader


@dataclass(frozen=True)
class FetchPartition:
    partition: int
    current_leader_epoch: int
    fetch_offset: int
    last_fetched_epoch: int
    log_start_offset: int
    partition_max_bytes: int

    @classmethod
    def read(cls, stream: BinaryIO) -> Self:
        p = cls(
            partition=read_int32(stream),
            current_leader_epoch=read_int32(stream),
            fetch_offset=read_int64(stream),
            last_fetched_epoch=read_int32(stream),
            log_start_offset=read_int64(stream),
            partition_max_bytes=read_int32(stream),
        )
        read_tagged_fields(stream)
        return p


@dataclass(frozen=True)
class FetchTopic:
    topic_id: UUID
    partitions: List[FetchPartition]

    @classmethod
    def read(cls, stream: BinaryIO) -> Self:
        topic_id = read_uuid(stream)
        partitions = read_compact_array(stream, FetchPartition.read)
        read_tagged_fields(stream)
        return cls(topic_id, partitions)


@dataclass(frozen=True)
class ForgottenTopic:
    topic_id: UUID
    partitions: List[int]

    @classmethod
    def read(cls, stream: BinaryIO) -> Self:
        topic_id = read_uuid(stream)
        partitions = read_compact_array(stream, read_int32)
        read_tagged_fields(stream)
        return cls(topic_id, partitions)


@dataclass(frozen=True)
class FetchRequest(AbstractRequest):
    max_wait_ms: int
    min_bytes: int
    max_bytes: int
    isolation_level: int
    session_id: int
    session_epoch: int
    topics: List[FetchTopic]
    forgotten_topics_data: List[ForgottenTopic]
    rack_id: str

    @classmethod
    def read(cls, stream: BinaryIO, header: RequestHeader) -> Self:
        request = cls(
            header=header,
            max_wait_ms=read_int32(stream),
            min_bytes=read_int32(stream),
            max_bytes=read_int32(stream),
            isolation_level=read_int8(stream),
            session_id=read_int32(stream),
            session_epoch=read_int32(stream),
            topics=read_compact_array(stream, FetchTopic.read),
            forgotten_topics_data=read_compact_array(stream, ForgottenTopic.read),
            rack_id=read_compact_string(stream),
        )
        read_tagged_fields(stream)
        return request
