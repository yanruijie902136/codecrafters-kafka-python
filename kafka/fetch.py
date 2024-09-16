from dataclasses import dataclass, field
from io import BytesIO
from uuid import UUID

from .constants import ErrorCode
from .primitive_types import *
from .request import Request, RequestBody
from .response import ResponseBody

###########
# Request #
###########


@dataclass
class TopicPartitionEntry:
    partition: int
    current_leader_epoch: int
    fetch_offset: int
    last_fetched_epoch: int
    log_start_offset: int
    partition_max_bytes: int

    @staticmethod
    def decode(byte_stream: BytesIO):
        partition = decode_int32(byte_stream)
        current_leader_epoch = decode_int32(byte_stream)
        fetch_offset = decode_int64(byte_stream)
        last_fetched_epoch = decode_int32(byte_stream)
        log_start_offset = decode_int64(byte_stream)
        partition_max_bytes = decode_int32(byte_stream)
        decode_tagged_fields(byte_stream)

        return TopicPartitionEntry(
            partition=partition,
            current_leader_epoch=current_leader_epoch,
            fetch_offset=fetch_offset,
            last_fetched_epoch=last_fetched_epoch,
            log_start_offset=log_start_offset,
            partition_max_bytes=partition_max_bytes,
        )


@dataclass
class TopicEntry:
    topic_id: UUID
    partitions: list[TopicPartitionEntry]

    @staticmethod
    def decode(byte_stream: BytesIO):
        topic_id = decode_uuid(byte_stream)
        partitions = decode_compact_array(byte_stream, TopicPartitionEntry.decode)
        decode_tagged_fields(byte_stream)

        return TopicEntry(topic_id=topic_id, partitions=partitions)


@dataclass
class ForgottonTopicEntry:
    topic_id: UUID
    partitions: list[int]

    @staticmethod
    def decode(byte_stream: BytesIO):
        topic_id = decode_uuid(byte_stream)
        partitions = decode_compact_array(byte_stream, decode_int32)
        decode_tagged_fields(byte_stream)

        return ForgottonTopicEntry(topic_id=topic_id, partitions=partitions)


@dataclass
class FetchRequestBody(RequestBody):
    max_wait_ms: int
    min_bytes: int
    max_bytes: int
    isolation_level: int
    session_id: int
    session_epoch: int
    topics: list[TopicEntry]
    forgotten_topics_data: list[ForgottonTopicEntry]
    rack_id: str

    @staticmethod
    def decode(byte_stream: BytesIO):
        max_wait_ms = decode_int32(byte_stream)
        min_bytes = decode_int32(byte_stream)
        max_bytes = decode_int32(byte_stream)
        isolation_level = decode_int8(byte_stream)
        session_id = decode_int32(byte_stream)
        session_epoch = decode_int32(byte_stream)
        topics = decode_compact_array(byte_stream, TopicEntry.decode)
        forgotten_topics_data = decode_compact_array(byte_stream, ForgottonTopicEntry.decode)
        rack_id = decode_compact_string(byte_stream)
        decode_tagged_fields(byte_stream)

        return FetchRequestBody(
            max_wait_ms=max_wait_ms,
            min_bytes=min_bytes,
            max_bytes=max_bytes,
            isolation_level=isolation_level,
            session_id=session_id,
            session_epoch=session_epoch,
            topics=topics,
            forgotten_topics_data=forgotten_topics_data,
            rack_id=rack_id,
        )


############
# Response #
############


@dataclass
class AbortedTransactionEntry:
    producer_id: int
    first_offset: int

    def encode(self):
        return b"".join([
            encode_int64(self.producer_id),
            encode_int64(self.first_offset),
            encode_tagged_fields(),
        ])


@dataclass
class ResponsePartitionEntry:
    partition_index: int
    error_code: ErrorCode
    high_watermark: int = 0
    last_stable_offset: int = 0
    log_start_offset: int = 0
    aborted_transactions: list[AbortedTransactionEntry] = field(default_factory=list)
    preferred_read_replica: int = 0

    def encode(self):
        return b"".join([
            encode_int32(self.partition_index),
            encode_int16(self.error_code),
            encode_int64(self.high_watermark),
            encode_int64(self.last_stable_offset),
            encode_int64(self.log_start_offset),
            encode_compact_array(self.aborted_transactions),
            encode_int32(self.preferred_read_replica),
            encode_tagged_fields(),  # FIXME: use \x00 for COMPACT_RECORDS for now.
            encode_tagged_fields(),
        ])


@dataclass
class ResponseEntry:
    topic_id: UUID
    partitions: list[ResponsePartitionEntry]

    def encode(self):
        return b"".join([
            encode_uuid(self.topic_id),
            encode_compact_array(self.partitions),
            encode_tagged_fields(),
        ])


@dataclass
class FetchResponseBody(ResponseBody):
    throttle_time_ms: int
    error_code: ErrorCode
    session_id: int
    responses: list[ResponseEntry]

    @staticmethod
    def from_request(request: Request):
        assert type(request.body) is FetchRequestBody, "Wrong request body!"

        if not request.body.topics:
            responses = []
        else:
            topic_entry = request.body.topics[0]
            responses = [
                ResponseEntry(
                    topic_id=topic_entry.topic_id,
                    partitions=[
                        ResponsePartitionEntry(partition_index=0, error_code=ErrorCode.UNKNOWN_TOPIC),
                    ]
                )
            ]

        return FetchResponseBody(
            throttle_time_ms=0,
            error_code=0,
            session_id=0,
            responses=responses,
        )

    def encode(self) -> bytes:
        return b"".join([
            encode_int32(self.throttle_time_ms),
            encode_int16(self.error_code),
            encode_int32(self.session_id),
            encode_compact_array(self.responses),
            encode_tagged_fields(),
        ])
