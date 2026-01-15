__all__ = (
    "BatchIndexAndErrorMessage",
    "PartitionProduceResponse",
    "ProduceResponse",
    "TopicProduceResponse",
)

from dataclasses import dataclass
from typing import List, Optional

from ..constants import ErrorCode
from ..primitives import encode_compact_array, encode_compact_string, encode_int32, encode_int64
from ..tagged_fields import encode_tagged_fields

from .abstract import AbstractResponse


@dataclass(frozen=True)
class BatchIndexAndErrorMessage:
    batch_index: int
    batch_index_error_message: Optional[str]

    def encode(self) -> bytes:
        return b"".join([
            encode_int32(self.batch_index),
            encode_compact_string(self.batch_index_error_message),
            encode_tagged_fields(),
        ])


@dataclass(frozen=True)
class PartitionProduceResponse:
    index: int
    error_code: ErrorCode
    base_offset: int
    log_append_time_ms: int
    log_start_offset: int
    record_errors: List[BatchIndexAndErrorMessage]
    error_message: Optional[str]

    def encode(self) -> bytes:
        return b"".join([
            encode_int32(self.index),
            self.error_code.encode(),
            encode_int64(self.base_offset),
            encode_int64(self.log_append_time_ms),
            encode_int64(self.log_start_offset),
            encode_compact_array(self.record_errors),
            encode_compact_string(self.error_message),
            encode_tagged_fields(),
        ])


@dataclass(frozen=True)
class TopicProduceResponse:
    name: str
    partition_responses: List[PartitionProduceResponse]

    def encode(self) -> bytes:
        return b"".join([
            encode_compact_string(self.name),
            encode_compact_array(self.partition_responses),
            encode_tagged_fields(),
        ])


@dataclass(frozen=True)
class ProduceResponse(AbstractResponse):
    responses: List[TopicProduceResponse]
    throttle_time_ms: int

    def encode(self) -> bytes:
        return b"".join([
            self.header.encode(version=1),
            encode_compact_array(self.responses),
            encode_int32(self.throttle_time_ms),
            encode_tagged_fields(),
        ])
