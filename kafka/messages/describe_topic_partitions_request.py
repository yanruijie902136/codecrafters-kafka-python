import dataclasses
import io
from typing import BinaryIO

from ..primitive_types import (
    decode_compact_array,
    decode_compact_string,
    decode_int32,
    decode_tagged_fields,
    encode_compact_string,
    encode_int32,
    encode_tagged_fields,
)

from .abstract_request import AbstractRequest


@dataclasses.dataclass
class DescribeTopicPartitionsRequestTopic:
    name: str

    @classmethod
    def decode(cls, binary_stream: BinaryIO):
        item = DescribeTopicPartitionsRequestTopic(
            name=decode_compact_string(binary_stream),
        )
        decode_tagged_fields(binary_stream)
        return item


@dataclasses.dataclass
class DescribeTopicPartitionsCursor:
    topic_name: str
    partition_index: int

    @classmethod
    def decode(cls, binary_stream: BinaryIO):
        if binary_stream.read(1) == b"\xff":
            return None
        binary_stream.seek(-1, io.SEEK_CUR)

        cursor = DescribeTopicPartitionsCursor(
            topic_name=decode_compact_string(binary_stream),
            partition_index=decode_int32(binary_stream),
        )
        decode_tagged_fields(binary_stream)
        return cursor

    def encode(self):
        return b"".join([
            encode_compact_string(self.topic_name),
            encode_int32(self.partition_index),
            encode_tagged_fields(),
        ])


@dataclasses.dataclass
class DescribeTopicPartitionsRequest(AbstractRequest):
    topics: list[DescribeTopicPartitionsRequestTopic]
    response_partition_limit: int
    cursor: DescribeTopicPartitionsCursor | None

    @classmethod
    def decode_body_kwargs(cls, binary_stream: BinaryIO):
        body_kwargs = {
            "topics":  decode_compact_array(binary_stream, DescribeTopicPartitionsRequestTopic.decode),
            "response_partition_limit": decode_int32(binary_stream),
            "cursor": DescribeTopicPartitionsCursor.decode(binary_stream),
        }
        decode_tagged_fields(binary_stream)
        return body_kwargs
