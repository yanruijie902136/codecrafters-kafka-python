import dataclasses
import uuid

from ..encode_functions import *
from ..error_code import ErrorCode
from ..request import FetchRequestBody, Request
from .response import ResponseBody


@dataclasses.dataclass
class AbortedTransactionItem:
    producer_id: int
    first_offset: int

    def encode(self):
        return b"".join([
            encode_int64(self.producer_id),
            encode_int64(self.first_offset),
            encode_tagged_fields(),
        ])


@dataclasses.dataclass
class PartitionItem:
    partition_index: int
    error_code: ErrorCode
    high_watermark: int = 0
    last_stable_offset: int = 0
    log_start_offset: int = 0
    aborted_transactions: list[AbortedTransactionItem] = dataclasses.field(default_factory=list)
    preferred_read_replica: int = 0

    def encode(self):
        return b"".join([
            encode_int32(self.partition_index),
            self.error_code.encode(),
            encode_int64(self.high_watermark),
            encode_int64(self.last_stable_offset),
            encode_int64(self.log_start_offset),
            encode_compact_array(self.aborted_transactions),
            encode_int32(self.preferred_read_replica),
            encode_tagged_fields(),  # FIXME: Use a null byte for COMPACT_RECORDS for now.
            encode_tagged_fields(),
        ])


@dataclasses.dataclass
class ResponseItem:
    topic_id: uuid.UUID
    partitions: list[PartitionItem]

    def encode(self):
        return b"".join([
            encode_uuid(self.topic_id),
            encode_compact_array(self.partitions),
            encode_tagged_fields(),
        ])


@dataclasses.dataclass
class FetchResponseBody(ResponseBody):
    throttle_time_ms: int
    error_code: ErrorCode
    session_id: int
    responses: list[ResponseItem]

    @staticmethod
    def from_request(request: Request):
        assert type(request.body) is FetchRequestBody, "Mismatched request body."

        if not request.body.topics:
            responses = []
        else:
            topic_item = request.body.topics[0]
            response_item = ResponseItem(
                topic_id=topic_item.topic_id,
                partitions=[
                    PartitionItem(partition_index=0, error_code=ErrorCode.UNKNOWN_TOPIC_ID),
                ],
            )
            responses = [response_item]

        return FetchResponseBody(
            throttle_time_ms=0,
            error_code=ErrorCode.NONE,
            session_id=0,
            responses=responses,
        )

    def encode(self):
        return b"".join([
            encode_int32(self.throttle_time_ms),
            self.error_code.encode(),
            encode_int32(self.session_id),
            encode_compact_array(self.responses),
            encode_tagged_fields(),
        ])
