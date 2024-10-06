from __future__ import annotations

import dataclasses
import uuid

from ...protocol import (
    ErrorCode,
    encode_compact_array,
    encode_int32,
    encode_int64,
    encode_tagged_fields,
    encode_uuid,
)

from ..request import Request
from ..response import AbstractResponseBody

from .request_body import FetchRequestBody


@dataclasses.dataclass
class FetchResponseBody(AbstractResponseBody):
    throttle_time_ms: int
    error_code: ErrorCode
    session_id: int
    responses: list[ResponseStruct]

    @classmethod
    def from_request(cls, request: Request) -> FetchResponseBody:
        assert type(request.body) is FetchRequestBody, "Mismatched request body."

        if not request.body.topics:
            responses = []
        else:
            topic_item = request.body.topics[0]
            response_item = ResponseStruct(
                topic_id=topic_item.topic_id,
                partitions=[
                    PartitionStruct(partition_index=0, error_code=ErrorCode.UNKNOWN_TOPIC_ID),
                ],
            )
            responses = [response_item]

        return FetchResponseBody(
            throttle_time_ms=0,
            error_code=ErrorCode.NONE,
            session_id=0,
            responses=responses,
        )

    def encode(self) -> bytes:
        return b"".join([
            encode_int32(self.throttle_time_ms),
            self.error_code.encode(),
            encode_int32(self.session_id),
            encode_compact_array(self.responses),
            encode_tagged_fields(),
        ])


@dataclasses.dataclass
class ResponseStruct:
    topic_id: uuid.UUID
    partitions: list[PartitionStruct]

    def encode(self) -> bytes:
        return b"".join([
            encode_uuid(self.topic_id),
            encode_compact_array(self.partitions),
            encode_tagged_fields(),
        ])


@dataclasses.dataclass
class PartitionStruct:
    partition_index: int
    error_code: ErrorCode
    high_watermark: int = 0
    last_stable_offset: int = 0
    log_start_offset: int = 0
    aborted_transactions: list[AbortedTransactionStruct] = dataclasses.field(default_factory=list)
    preferred_read_replica: int = 0

    def encode(self) -> bytes:
        return b"".join([
            encode_int32(self.partition_index),
            self.error_code.encode(),
            encode_int64(self.high_watermark),
            encode_int64(self.last_stable_offset),
            encode_int64(self.log_start_offset),
            encode_compact_array(self.aborted_transactions),
            encode_int32(self.preferred_read_replica),
            encode_compact_array([]),
            encode_tagged_fields(),
        ])


@dataclasses.dataclass
class AbortedTransactionStruct:
    producer_id: int
    first_offset: int

    def encode(self) -> bytes:
        return b"".join([
            encode_int64(self.producer_id),
            encode_int64(self.first_offset),
            encode_tagged_fields(),
        ])
