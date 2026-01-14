__all__ = "handle_describe_topic_partitions_request",

from uuid import UUID

from ..protocol.constants import ErrorCode
from ..protocol.requests import DescribeTopicPartitionsRequest
from ..protocol.requests.describe_topic_partitions import TopicRequest
from ..protocol.responses import DescribeTopicPartitionsResponse
from ..protocol.responses.describe_topic_partitions import ResponseTopic

from .utils import make_response_header


def _make_response_topic(topic: TopicRequest) -> ResponseTopic:
    return ResponseTopic(
        error_code=ErrorCode.UNKNOWN_TOPIC_OR_PARTITION,
        name=topic.name,
        topic_id=UUID(int=0),
        is_internal=False,
        partitions=[],
        topic_authorized_operations=0,
    )


def handle_describe_topic_partitions_request(request: DescribeTopicPartitionsRequest) -> DescribeTopicPartitionsResponse:
    return DescribeTopicPartitionsResponse(
        header=make_response_header(request),
        throttle_time_ms=0,
        topics=[_make_response_topic(topic) for topic in request.topics],
        next_cursor=None,
    )
