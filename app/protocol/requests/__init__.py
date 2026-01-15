__all__ = (
    "AbstractRequest",
    "ApiVersionsRequest",
    "DescribeTopicPartitionsRequest",
    "FetchRequest",
    "ProduceRequest",
    "RequestHeader",
    "read_request",
)

from typing import BinaryIO

from ..constants import ApiKey

from .abstract import AbstractRequest
from .api_versions import ApiVersionsRequest
from .describe_topic_partitions import DescribeTopicPartitionsRequest
from .fetch import FetchRequest
from .header import RequestHeader
from .produce import ProduceRequest


def read_request(stream: BinaryIO) -> AbstractRequest:
    header = RequestHeader.read(stream)
    match header.api_key:
        case ApiKey.API_VERSIONS:
            return ApiVersionsRequest.read(stream, header)
        case ApiKey.DESCRIBE_TOPIC_PARTITIONS:
            return DescribeTopicPartitionsRequest.read(stream, header)
        case ApiKey.FETCH:
            return FetchRequest.read(stream, header)
        case ApiKey.PRODUCE:
            return ProduceRequest.read(stream, header)
