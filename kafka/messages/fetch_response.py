import dataclasses
import uuid

from ..metadata import ClusterMetadata, RecordBatch, read_record_batches
from ..protocol import (
    ErrorCode,
    encode_compact_array,
    encode_int32,
    encode_int64,
    encode_tagged_fields,
    encode_uuid,
)

from .abstract_response import AbstractResponse
from .fetch_request import FetchRequest, FetchRequestTopic


@dataclasses.dataclass
class FetchResponseAbortedTransaction:
    producer_id: int
    first_offset: int

    def encode(self):
        return b"".join([
            encode_int64(self.producer_id),
            encode_int64(self.first_offset),
            encode_tagged_fields(),
        ])


@dataclasses.dataclass
class FetchResponsePartition:
    partition_index: int
    error_code: ErrorCode
    high_watermark: int = 0
    last_stable_offset: int = 0
    log_start_offset: int = 0
    aborted_transactions: list[FetchResponseAbortedTransaction] = dataclasses.field(default_factory=list)
    preferred_read_replica: int = 0
    records: list[RecordBatch] = dataclasses.field(default_factory=list)

    def encode(self):
        return b"".join([
            encode_int32(self.partition_index),
            self.error_code.encode(),
            encode_int64(self.high_watermark),
            encode_int64(self.last_stable_offset),
            encode_int64(self.log_start_offset),
            encode_compact_array(self.aborted_transactions),
            encode_int32(self.preferred_read_replica),
            encode_compact_array(self.records),
            encode_tagged_fields(),
        ])


@dataclasses.dataclass
class FetchResponseTopic:
    topic_id: uuid.UUID
    partitions: list[FetchResponsePartition]

    @classmethod
    def from_request_topic(cls, request_topic: FetchRequestTopic):
        cluster_metadata = ClusterMetadata()
        topic_name = cluster_metadata.get_topic_name(request_topic.topic_id)
        if topic_name is None:
            return FetchResponseTopic(
                topic_id=request_topic.topic_id,
                partitions=[
                    FetchResponsePartition(
                        partition_index=0,
                        error_code=ErrorCode.UNKNOWN_TOPIC_ID,
                    ),
                ],
            )

        return FetchResponseTopic(
            topic_id=request_topic.topic_id,
            partitions=[
                FetchResponsePartition(
                    partition_index=p.partition,
                    error_code=ErrorCode.NONE,
                    records=list(read_record_batches(topic_name, p.partition)),
                )
                for p in request_topic.partitions
            ],
        )

    def encode(self):
        return b"".join([
            encode_uuid(self.topic_id),
            encode_compact_array(self.partitions),
            encode_tagged_fields(),
        ])


@dataclasses.dataclass
class FetchResponse(AbstractResponse):
    throttle_time_ms: int
    error_code: ErrorCode
    session_id: int
    responses: list[FetchResponseTopic]

    @classmethod
    def make_body_kwargs(cls, request: FetchRequest):
        return {
            "throttle_time_ms": 0,
            "error_code": ErrorCode.NONE,
            "session_id": 0,
            "responses": [FetchResponseTopic.from_request_topic(topic) for topic in request.topics],
        }

    def _encode_body(self):
        return b"".join([
            encode_int32(self.throttle_time_ms),
            self.error_code.encode(),
            encode_int32(self.session_id),
            encode_compact_array(self.responses),
            encode_tagged_fields(),
        ])
