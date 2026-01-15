__all__ = "handle_fetch_request",

from ..cluster_metadata import ClusterMetadata
from ..protocol.constants import ErrorCode
from ..protocol.requests import FetchRequest
from ..protocol.requests.fetch import FetchTopic
from ..protocol.responses import FetchResponse
from ..protocol.responses.fetch import FetchableTopicResponse, PartitionData

from .utils import make_response_header


def _get_records(topic_name: str, partition_index: int) -> bytes:
    path = f"/tmp/kraft-combined-logs/{topic_name}-{partition_index}/00000000000000000000.log"
    with open(path, mode="rb") as f:
        return f.read()


def _make_fetchable_topic_response(fetch_topic: FetchTopic) -> FetchableTopicResponse:
    topic_id = fetch_topic.topic_id
    topic = ClusterMetadata().get_topic(topic_id=topic_id)
    if topic is None:
        return FetchableTopicResponse(
            topic_id=topic_id,
            partitions=[
                PartitionData(
                    partition_index=0,
                    error_code=ErrorCode.UNKNOWN_TOPIC_ID,
                    high_watermark=0,
                    last_stable_offset=0,
                    log_start_offset=0,
                    aborted_transactions=None,
                    preferred_read_replica=0,
                    records=None,
                ),
            ],
        )

    return FetchableTopicResponse(
        topic_id=topic_id,
        partitions=[
            PartitionData(
                partition_index=p.partition_index,
                error_code=ErrorCode.NONE,
                high_watermark=0,
                last_stable_offset=0,
                log_start_offset=0,
                aborted_transactions=None,
                preferred_read_replica=0,
                records=_get_records(topic.name, p.partition_index),
            )
            for p in topic.partitions.values()
        ],
    )


def handle_fetch_request(request: FetchRequest) -> FetchResponse:
    return FetchResponse(
        header=make_response_header(request),
        throttle_time_ms=0,
        error_code=ErrorCode.NONE,
        session_id=request.session_id,
        responses=[_make_fetchable_topic_response(fetch_topic) for fetch_topic in request.topics],
    )
