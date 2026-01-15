__all__ = (
    "AbortedTransaction",
    "FetchResponse",
    "FetchableTopicResponse",
    "PartitionData",
)

from dataclasses import dataclass
from typing import List, Optional
from uuid import UUID

from ..constants import ErrorCode
from ..primitives import encode_compact_array, encode_int32, encode_int64, encode_uuid
from ..tagged_fields import encode_tagged_fields

from .abstract import AbstractResponse


@dataclass(frozen=True)
class AbortedTransaction:
    producer_id: int
    first_offset: int

    def encode(self) -> bytes:
        return b"".join([
            encode_int64(self.producer_id),
            encode_int64(self.first_offset),
            encode_tagged_fields(),
        ])


@dataclass(frozen=True)
class PartitionData:
    partition_index: int
    error_code: ErrorCode
    high_watermark: int
    last_stable_offset: int
    log_start_offset: int
    aborted_transactions: Optional[List[AbortedTransaction]]
    preferred_read_replica: int
    # TODO: add records field

    def encode(self) -> bytes:
        return b"".join([
            encode_int32(self.partition_index),
            self.error_code.encode(),
            encode_int64(self.high_watermark),
            encode_int64(self.last_stable_offset),
            encode_int64(self.log_start_offset),
            encode_compact_array(self.aborted_transactions),
            encode_int32(self.preferred_read_replica),
            # TODO: encode records field
            encode_tagged_fields(),
        ])


@dataclass(frozen=True)
class FetchableTopicResponse:
    topic_id: UUID
    partitions: List[PartitionData]

    def encode(self) -> bytes:
        return b"".join([
            encode_uuid(self.topic_id),
            encode_compact_array(self.partitions),
            encode_tagged_fields(),
        ])


@dataclass(frozen=True)
class FetchResponse(AbstractResponse):
    throttle_time_ms: int
    error_code: ErrorCode
    session_id: int
    responses: List[FetchableTopicResponse]

    def encode(self) -> bytes:
        return b"".join([
            self.header.encode(version=1),
            encode_int32(self.throttle_time_ms),
            self.error_code.encode(),
            encode_int32(self.session_id),
            encode_compact_array(self.responses),
            encode_tagged_fields(),
        ])
