__all__ = (
    "handle_api_versions_request",
    "handle_describe_topic_partitions_request",
    "handle_fetch_request",
    "handle_produce_request",
    "handle_request",
)

from ..protocol.requests import (
    AbstractRequest,
    ApiVersionsRequest,
    DescribeTopicPartitionsRequest,
    FetchRequest,
    ProduceRequest,
)
from ..protocol.responses import AbstractResponse

from .api_versions import handle_api_versions_request
from .describe_topic_partitions import handle_describe_topic_partitions_request
from .fetch import handle_fetch_request
from .produce import handle_produce_request


def handle_request(request: AbstractRequest) -> AbstractResponse:
    if isinstance(request, ApiVersionsRequest):
        return handle_api_versions_request(request)
    elif isinstance(request, DescribeTopicPartitionsRequest):
        return handle_describe_topic_partitions_request(request)
    elif isinstance(request, FetchRequest):
        return handle_fetch_request(request)
    elif isinstance(request, ProduceRequest):
        return handle_produce_request(request)
    else:
        raise TypeError(f"Unsupported request type: {type(request).__name__}")
