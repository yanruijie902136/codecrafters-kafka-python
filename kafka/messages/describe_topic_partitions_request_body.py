from __future__ import annotations

import dataclasses
import io

from ..primitive_types import (
    decode_compact_array,
    decode_compact_string,
    decode_int32,
    decode_tagged_fields,
)

from .abstract_request_body import AbstractRequestBody


@dataclasses.dataclass
class DescribeTopicPartitionsRequestBody(AbstractRequestBody):
    topics: list[TopicStruct]
    response_partition_limit: int

    @classmethod
    def decode(cls, byte_stream: io.BufferedIOBase) -> DescribeTopicPartitionsRequestBody:
        request_body = DescribeTopicPartitionsRequestBody(
            topics=decode_compact_array(byte_stream, TopicStruct.decode),
            response_partition_limit=decode_int32(byte_stream),
        )

        assert byte_stream.read(1) == b"\xff", "Only null cursor is supported."

        decode_tagged_fields(byte_stream)
        return request_body


@dataclasses.dataclass
class TopicStruct:
    name: str

    @classmethod
    def decode(cls, byte_stream: io.BufferedIOBase) -> TopicStruct:
        item = TopicStruct(
            name=decode_compact_string(byte_stream),
        )
        decode_tagged_fields(byte_stream)
        return item
