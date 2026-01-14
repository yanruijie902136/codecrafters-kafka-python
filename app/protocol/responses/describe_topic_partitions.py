__all__ = (
    "Cursor",
    "DescribeTopicPartitionsResponse",
    "ResponsePartition",
    "ResponseTopic",
)

from dataclasses import dataclass
from typing import List, Optional
from uuid import UUID

from ..constants import ErrorCode
from ..primitives import (
    encode_boolean,
    encode_compact_array,
    encode_compact_string,
    encode_int32,
    encode_int8,
    encode_uuid,
)
from ..tagged_fields import encode_tagged_fields

from .abstract import AbstractResponse


@dataclass(frozen=True)
class ResponsePartition:
    error_code: ErrorCode
    partition_index: int
    leader_id: int
    leader_epoch: int
    replica_nodes: List[int]
    isr_nodes: List[int]
    eligible_leader_replicas: Optional[List[int]]
    last_known_elr: Optional[List[int]]
    offline_replicas: List[int]

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


@dataclass(frozen=True)
class ResponseTopic:
    error_code: ErrorCode
    name: Optional[str]
    topic_id: UUID
    is_internal: bool
    partitions: List[ResponsePartition]
    topic_authorized_operations: int

    def encode(self) -> bytes:
        return b"".join([
            self.error_code.encode(),
            encode_compact_string(self.name),
            encode_uuid(self.topic_id),
            encode_boolean(self.is_internal),
            encode_compact_array(self.partitions),
            encode_int32(self.topic_authorized_operations),
            encode_tagged_fields(),
        ])


@dataclass(frozen=True)
class Cursor:
    topic_name: str
    partition_index: int

    def encode(self) -> bytes:
        return b"".join([
            encode_compact_string(self.topic_name),
            encode_int32(self.partition_index),
            encode_tagged_fields(),
        ])


@dataclass(frozen=True)
class DescribeTopicPartitionsResponse(AbstractResponse):
    throttle_time_ms: int
    topics: List[ResponseTopic]
    next_cursor: Optional[Cursor]

    def encode(self) -> bytes:
        data = b"".join([
            self.header.encode(version=1),
            encode_int32(self.throttle_time_ms),
            encode_compact_array(self.topics),
        ])
        if self.next_cursor is None:
            data += encode_int8(-1)
        else:
            data += encode_int8(1) + self.next_cursor.encode()
        data += encode_tagged_fields()
        return data
