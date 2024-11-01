import dataclasses
import uuid
from typing import BinaryIO

from ..primitive_types import (
    decode_compact_array,
    decode_compact_string,
    decode_int8,
    decode_int32,
    decode_int64,
    decode_tagged_fields,
    decode_uuid,
)

from .abstract_request import AbstractRequest


@dataclasses.dataclass
class FetchRequestPartition:
    partition: int
    current_leader_epoch: int
    fetch_offset: int
    last_fetched_epoch: int
    log_start_offset: int
    partition_max_bytes: int

    @classmethod
    def decode(cls, binary_stream: BinaryIO):
        item = FetchRequestPartition(
            partition=decode_int32(binary_stream),
            current_leader_epoch=decode_int32(binary_stream),
            fetch_offset=decode_int64(binary_stream),
            last_fetched_epoch=decode_int32(binary_stream),
            log_start_offset=decode_int64(binary_stream),
            partition_max_bytes=decode_int32(binary_stream),
        )
        decode_tagged_fields(binary_stream)
        return item


@dataclasses.dataclass
class FetchRequestTopic:
    topic_id: uuid.UUID
    partitions: list[FetchRequestPartition]

    @classmethod
    def decode(cls, binary_stream: BinaryIO):
        item = FetchRequestTopic(
            topic_id=decode_uuid(binary_stream),
            partitions=decode_compact_array(binary_stream, FetchRequestPartition.decode),
        )
        decode_tagged_fields(binary_stream)
        return item


@dataclasses.dataclass
class FetchRequestForgottenTopic:
    topic_id: uuid.UUID
    partitions: list[int]

    @classmethod
    def decode(cls, binary_stream: BinaryIO):
        item = FetchRequestForgottenTopic(
            topic_id=decode_uuid(binary_stream),
            partitions=decode_compact_array(binary_stream, decode_int32),
        )
        decode_tagged_fields(binary_stream)
        return item


@dataclasses.dataclass
class FetchRequest(AbstractRequest):
    max_wait_ms: int
    min_bytes: int
    max_bytes: int
    isolation_level: int
    session_id: int
    session_epoch: int
    topics: list[FetchRequestTopic]
    forgotten_topics_data: list[FetchRequestForgottenTopic]
    rack_id: str

    @classmethod
    def decode_body_kwargs(cls, binary_stream: BinaryIO):
        body_kwargs = {
            "max_wait_ms": decode_int32(binary_stream),
            "min_bytes": decode_int32(binary_stream),
            "max_bytes": decode_int32(binary_stream),
            "isolation_level": decode_int8(binary_stream),
            "session_id": decode_int32(binary_stream),
            "session_epoch": decode_int32(binary_stream),
            "topics": decode_compact_array(binary_stream, FetchRequestTopic.decode),
            "forgotten_topics_data": decode_compact_array(binary_stream, FetchRequestForgottenTopic.decode),
            "rack_id": decode_compact_string(binary_stream),
        }
        decode_tagged_fields(binary_stream)
        return body_kwargs
