__all__ = "handle_fetch_request",

from ..protocol.constants import ErrorCode
from ..protocol.requests import FetchRequest
from ..protocol.requests.fetch import FetchTopic
from ..protocol.responses import FetchResponse
from ..protocol.responses.fetch import FetchableTopicResponse, PartitionData

from .utils import make_response_header


def _make_fetchable_topic_response(fetch_topic: FetchTopic) -> FetchableTopicResponse:
    return FetchableTopicResponse(
        topic_id=fetch_topic.topic_id,
        partitions=[
            PartitionData(
                partition_index=0,
                error_code=ErrorCode.UNKNOWN_TOPIC_ID,
                high_watermark=0,
                last_stable_offset=0,
                log_start_offset=0,
                aborted_transactions=None,
                preferred_read_replica=0,
            ),
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
