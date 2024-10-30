import dataclasses

from ..primitive_types import (
    BinaryStream,
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
    def decode(cls, binary_stream: BinaryStream):
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
    def decode(cls, binary_stream: BinaryStream):
        assert binary_stream.read(1) == b"\xff", "Unexpected cursor."
        return None

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
    def decode_body_kwargs(cls, binary_stream: BinaryStream):
        body_kwargs = {
            "topics":  decode_compact_array(binary_stream, DescribeTopicPartitionsRequestTopic.decode),
            "response_partition_limit": decode_int32(binary_stream),
            "cursor": DescribeTopicPartitionsCursor.decode(binary_stream),
        }
        decode_tagged_fields(binary_stream)
        return body_kwargs
