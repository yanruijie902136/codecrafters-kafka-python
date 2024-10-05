from __future__ import annotations

import dataclasses
import io

from .cursor import Cursor
from ..decode_functions import (
    decode_compact_array,
    decode_compact_string,
    decode_int32,
    decode_tagged_fields,
)
from ..request import KafkaRequestBody


@dataclasses.dataclass
class DescribeTopicPartitionsRequestBody(KafkaRequestBody):
    topics: list[TopicItem]
    response_partition_limit: int
    cursor: Cursor

    @classmethod
    def decode(cls, byte_stream: io.BytesIO) -> DescribeTopicPartitionsRequestBody:
        body = DescribeTopicPartitionsRequestBody(
            topics=decode_compact_array(byte_stream, TopicItem.decode),
            response_partition_limit=decode_int32(byte_stream),
            cursor=Cursor.decode(byte_stream),
        )
        decode_tagged_fields(byte_stream)
        return body


@dataclasses.dataclass
class TopicItem:
    name: str

    @classmethod
    def decode(cls, byte_stream: io.BytesIO) -> TopicItem:
        item = TopicItem(
            name=decode_compact_string(byte_stream),
        )
        decode_tagged_fields(byte_stream)
        return item
