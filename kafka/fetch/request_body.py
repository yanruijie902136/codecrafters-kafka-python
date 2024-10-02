from __future__ import annotations

import dataclasses
import io
import uuid

from ..decode_functions import (
    decode_compact_array,
    decode_compact_string,
    decode_int8,
    decode_int32,
    decode_int64,
    decode_tagged_fields,
    decode_uuid,
)
from ..request import KafkaRequestBody


@dataclasses.dataclass
class FetchRequestBody(KafkaRequestBody):
    max_wait_ms: int
    min_bytes: int
    max_bytes: int
    isolation_level: int
    session_id: int
    session_epoch: int
    topics: list[TopicItem]
    forgotten_topics_data: list[ForgottenTopicItem]
    rack_id: str

    @classmethod
    def decode(cls, byte_stream: io.BytesIO) -> FetchRequestBody:
        body = FetchRequestBody(
            max_wait_ms=decode_int32(byte_stream),
            min_bytes=decode_int32(byte_stream),
            max_bytes=decode_int32(byte_stream),
            isolation_level=decode_int8(byte_stream),
            session_id=decode_int32(byte_stream),
            session_epoch=decode_int32(byte_stream),
            topics=decode_compact_array(byte_stream, TopicItem.decode),
            forgotten_topics_data=decode_compact_array(byte_stream, ForgottenTopicItem.decode),
            rack_id=decode_compact_string(byte_stream),
        )
        decode_tagged_fields(byte_stream)
        return body


@dataclasses.dataclass
class TopicItem:
    topic_id: uuid.UUID
    partitions: list[PartitionItem]

    @classmethod
    def decode(cls, byte_stream: io.BytesIO) -> TopicItem:
        item = TopicItem(
            topic_id=decode_uuid(byte_stream),
            partitions=decode_compact_array(byte_stream, PartitionItem.decode),
        )
        decode_tagged_fields(byte_stream)
        return item


@dataclasses.dataclass
class PartitionItem:
    partition: int
    current_leader_epoch: int
    fetch_offset: int
    last_fetched_epoch: int
    log_start_offset: int
    partition_max_bytes: int

    @classmethod
    def decode(cls, byte_stream: io.BytesIO) -> PartitionItem:
        item = PartitionItem(
            partition=decode_int32(byte_stream),
            current_leader_epoch=decode_int32(byte_stream),
            fetch_offset=decode_int64(byte_stream),
            last_fetched_epoch=decode_int32(byte_stream),
            log_start_offset=decode_int64(byte_stream),
            partition_max_bytes=decode_int32(byte_stream),
        )
        decode_tagged_fields(byte_stream)
        return item


@dataclasses.dataclass
class ForgottenTopicItem:
    topic_id: uuid.UUID
    partitions: list[int]

    @classmethod
    def decode(cls, byte_stream: io.BytesIO) -> ForgottenTopicItem:
        item = ForgottenTopicItem(
            topic_id=decode_uuid(byte_stream),
            partitions=decode_compact_array(byte_stream, decode_int32),
        )
        decode_tagged_fields(byte_stream)
        return item
