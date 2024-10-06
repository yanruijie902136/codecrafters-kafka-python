from __future__ import annotations

import dataclasses
import io

from ...protocol import (
    decode_compact_array,
    decode_compact_string,
    decode_int32,
    decode_tagged_fields,
)

from ..request import AbstractRequestBody


@dataclasses.dataclass
class DescribeTopicPartitionsRequestBody(AbstractRequestBody):
    topics: list[TopicStruct]
    response_partition_limit: int

    @classmethod
    def decode(cls, byte_stream: io.BytesIO) -> DescribeTopicPartitionsRequestBody:
        body = DescribeTopicPartitionsRequestBody(
            topics=decode_compact_array(byte_stream, TopicStruct.decode),
            response_partition_limit=decode_int32(byte_stream),
        )
        assert byte_stream.read(1) == b"\xff", "Only null cursor is supported."
        decode_tagged_fields(byte_stream)
        return body


@dataclasses.dataclass
class TopicStruct:
    name: str

    @classmethod
    def decode(cls, byte_stream: io.BytesIO) -> TopicStruct:
        item = TopicStruct(
            name=decode_compact_string(byte_stream),
        )
        decode_tagged_fields(byte_stream)
        return item
