from __future__ import annotations

import dataclasses
import uuid

from ...protocol import (
    ErrorCode,
    encode_boolean,
    encode_compact_array,
    encode_compact_nullable_string,
    encode_int32,
    encode_tagged_fields,
    encode_uuid,
)
from ...records import RecordManager

from ..abstract_response_body import AbstractResponseBody
from .describe_topic_partitions_request_body import DescribeTopicPartitionsRequestBody
from ..request import Request


@dataclasses.dataclass
class DescribeTopicPartitionsResponseBody(AbstractResponseBody):
    throttle_time_ms: int
    topics: list[TopicStruct]

    @classmethod
    def from_request(cls, request: Request) -> DescribeTopicPartitionsResponseBody:
        assert type(request.body) is DescribeTopicPartitionsRequestBody, "Mismatched request body."

        record_manager = RecordManager()
        topic_responses = []
        for topic in request.body.topics:
            if (topic_record := record_manager.get_topic(topic.name)) is None:
                topic_item = TopicStruct(
                    error_code=ErrorCode.UNKNOWN_TOPIC_OR_PARTITION,
                    name=topic.name,
                    topic_id=uuid.UUID(int=0),
                    is_internal=False,
                    partitions=[],
                    topic_authorized_operations=0,
                )
            else:
                topic_id = topic_record.topic_id
                partitions = [
                    PartitionStruct(error_code=ErrorCode.NONE, partition_index=partition_record.partition_id)
                    for partition_record in record_manager.get_partitions(topic_id)
                ]
                partitions.sort(key=lambda p: p.partition_index)

                topic_item = TopicStruct(
                    error_code=ErrorCode.NONE,
                    name=topic.name,
                    topic_id=topic_id,
                    is_internal=False,
                    partitions=partitions,
                    topic_authorized_operations=0,
                )
            topic_responses.append(topic_item)

        return DescribeTopicPartitionsResponseBody(
            throttle_time_ms=0,
            topics=topic_responses,
        )

    def encode(self) -> bytes:
        return b"".join([
            encode_int32(self.throttle_time_ms),
            encode_compact_array(self.topics),
            b"\xff",
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
    leader_id: int = 0
    leader_epoch: int = 0
    replica_nodes: list[int] = dataclasses.field(default_factory=list)
    isr_nodes: list[int] = dataclasses.field(default_factory=list)
    eligible_leader_replicas: list[int] = dataclasses.field(default_factory=list)
    last_known_elr: list[int] = dataclasses.field(default_factory=list)
    offline_replicas: list[int] = dataclasses.field(default_factory=list)

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
