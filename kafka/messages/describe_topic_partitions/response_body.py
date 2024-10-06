from __future__ import annotations

import dataclasses
import uuid

from ...encode_functions import (
    encode_boolean,
    encode_compact_array,
    encode_compact_nullable_string,
    encode_int32,
    encode_tagged_fields,
    encode_uuid,
)
from ...error_code import ErrorCode

from ..request import Request
from ..response import AbstractResponseBody

from .request_body import DescribeTopicPartitionsRequestBody


@dataclasses.dataclass
class DescribeTopicPartitionsResponseBody(AbstractResponseBody):
    throttle_time_ms: int
    topics: list[TopicStruct]

    @classmethod
    def from_request(cls, request: Request) -> DescribeTopicPartitionsResponseBody:
        assert type(request.body) is DescribeTopicPartitionsRequestBody, "Mismatched request body."

        topics = [
            TopicStruct(
                error_code=ErrorCode.UNKNOWN_TOPIC_OR_PARTITION,
                name=request.body.topics[0].name,
                topic_id=uuid.UUID(int=0),
                is_internal=False,
                partitions=[],
                topic_authorized_operations=0,
            ),
        ]

        return DescribeTopicPartitionsResponseBody(
            throttle_time_ms=0,
            topics=topics,
        )

    def encode(self) -> bytes:
        return b"".join([
            encode_int32(self.throttle_time_ms),
            encode_compact_array(self.topics),
            b"\xff",    # FIXME: Only support null cursor (a 0xff byte) for now.
            encode_tagged_fields(),
        ])


@dataclasses.dataclass
class TopicStruct:
    error_code: ErrorCode
    name: str
    topic_id: uuid.UUID
    is_internal: bool
    partitions: list[PartitionStruct]
    topic_authorized_operations: int

    def encode(self) -> bytes:
        return b"".join([
            self.error_code.encode(),
            encode_compact_nullable_string(self.name),
            encode_uuid(self.topic_id),
            encode_boolean(self.is_internal),
            encode_compact_array(self.partitions),
            encode_int32(self.topic_authorized_operations),
            encode_tagged_fields(),
        ])


@dataclasses.dataclass
class PartitionStruct:
    error_code: ErrorCode
    partition_index: int
    leader_id: int
    leader_epoch: int
    replica_nodes: list[int]
    isr_nodes: list[int]
    eligible_leader_replicas: list[int]
    last_known_elr: list[int]
    offline_replicas: list[int]

    def encode(self) -> bytes:
        return b"".join([
            self.error_code.encode(),
            encode_int32(self.partition_index),
            encode_int32(self.leader_id),
            encode_int32(self.leader_epoch),
            encode_compact_array(self.replica_nodes, encode_int32),
            encode_compact_array(self.isr_nodes, encode_int32),
            encode_compact_array(self.eligible_leader_replicas, encode_int32),
            encode_compact_array(self.last_known_elr, encode_int32),
            encode_compact_array(self.offline_replicas, encode_int32),
            encode_tagged_fields(),
        ])
