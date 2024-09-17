import dataclasses
import io
import uuid

from ..decode_functions import *
from .request import RequestBody


@dataclasses.dataclass
class PartitionItem:
    partition: int
    current_leader_epoch: int
    fetch_offset: int
    last_fetched_epoch: int
    log_start_offset: int
    partition_max_bytes: int

    @staticmethod
    def decode(byte_stream: io.BytesIO):
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
class TopicItem:
    topic_id: uuid.UUID
    partitions: list[PartitionItem]

    @staticmethod
    def decode(byte_stream: io.BytesIO):
        item = TopicItem(
            topic_id=decode_uuid(byte_stream),
            partitions=decode_compact_array(byte_stream, PartitionItem.decode),
        )
        decode_tagged_fields(byte_stream)
        return item


@dataclasses.dataclass
class ForgottenTopicItem:
    topic_id: uuid.UUID
    partitions: list[int]

    @staticmethod
    def decode(byte_stream: io.BytesIO):
        item = ForgottenTopicItem(
            topic_id=decode_uuid(byte_stream),
            partitions=decode_compact_array(byte_stream, decode_int32),
        )
        decode_tagged_fields(byte_stream)
        return item


@dataclasses.dataclass
class FetchRequestBody(RequestBody):
    max_wait_ms: int
    min_bytes: int
    max_bytes: int
    isolation_level: int
    session_id: int
    session_epoch: int
    topics: list[TopicItem]
    forgotten_topics_data: list[ForgottenTopicItem]
    rack_id: str

    @staticmethod
    def decode(byte_stream: io.BytesIO):
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
