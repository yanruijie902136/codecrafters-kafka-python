import dataclasses
import uuid

from ..metadata import ClusterMetadata
from ..protocol import (
    ErrorCode,
    encode_boolean,
    encode_compact_array,
    encode_compact_nullable_string,
    encode_int32,
    encode_tagged_fields,
    encode_uuid,
)

from .abstract_response import AbstractResponse
from .describe_topic_partitions_request import (
    DescribeTopicPartitionsCursor,
    DescribeTopicPartitionsRequest,
)


@dataclasses.dataclass
class DescribeTopicPartitionsResponsePartition:
    error_code: ErrorCode
    partition_index: int
    leader_id: int = 0
    leader_epoch: int = 0
    replica_nodes: list[int] = dataclasses.field(default_factory=list)
    isr_nodes: list[int] = dataclasses.field(default_factory=list)
    eligible_leader_replicas: list[int] = dataclasses.field(default_factory=list)
    last_known_elr: list[int] = dataclasses.field(default_factory=list)
    offline_replicas: list[int] = dataclasses.field(default_factory=list)

    def encode(self):
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


@dataclasses.dataclass
class DescribeTopicPartitionsResponseTopic:
    error_code: ErrorCode
    name: str
    topic_id: uuid.UUID
    is_internal: bool
    partitions: list[DescribeTopicPartitionsResponsePartition]
    topic_authorized_operations: int

    @classmethod
    def from_topic_name(cls, topic_name: str):
        cluster_metadata = ClusterMetadata()
        topic_id = cluster_metadata.get_topic_id(topic_name)
        if topic_id is None:
            return DescribeTopicPartitionsResponseTopic(
                error_code=ErrorCode.UNKNOWN_TOPIC_OR_PARTITION,
                name=topic_name,
                topic_id=uuid.UUID(int=0),
                is_internal=False,
                partitions=[],
                topic_authorized_operations=0,
            )

        return DescribeTopicPartitionsResponseTopic(
            error_code=ErrorCode.NONE,
            name=topic_name,
            topic_id=topic_id,
            is_internal=False,
            partitions=[
                DescribeTopicPartitionsResponsePartition(
                    error_code=ErrorCode.NONE,
                    partition_index=partition_index,
                )
                for partition_index in cluster_metadata.get_partition_indices(topic_id)
            ],
            topic_authorized_operations=0,
        )

    def encode(self):
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
class DescribeTopicPartitionsResponse(AbstractResponse):
    throttle_time_ms: int
    topics: list[DescribeTopicPartitionsResponseTopic]
    next_cursor: DescribeTopicPartitionsCursor | None

    @classmethod
    def make_body_kwargs(cls, request: DescribeTopicPartitionsRequest):
        return {
            "throttle_time_ms": 0,
            "topics": [DescribeTopicPartitionsResponseTopic.from_topic_name(topic.name) for topic in request.topics],
            "next_cursor": request.cursor,
        }

    def _encode_body(self):
        return b"".join([
            encode_int32(self.throttle_time_ms),
            encode_compact_array(self.topics),
            b"\xff" if self.next_cursor is None else self.next_cursor.encode(),
            encode_tagged_fields(),
        ])
