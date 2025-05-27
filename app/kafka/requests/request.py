from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Self

from ..protocol import *


@dataclass(frozen=True)
class RequestHeader:
    request_api_key: ApiKey
    request_api_version: int
    correlation_id: int
    client_id: str | None

    @classmethod
    def decode(cls, readable: Readable) -> Self:
        request_header = cls(
            request_api_key=ApiKey.decode(readable),
            request_api_version=decode_int16(readable),
            correlation_id=decode_int32(readable),
            client_id=decode_nullable_string(readable),
        )
        decode_tagged_fields(readable)
        return request_header


@dataclass(frozen=True)
class Request(ABC):
    header: RequestHeader

    @classmethod
    @abstractmethod
    def decode_body(cls, header: RequestHeader, readable: Readable) -> Self:
        raise NotImplementedError


def decode_request(readable: Readable) -> Request:
    header = RequestHeader.decode(readable)

    match header.request_api_key:
        case ApiKey.FETCH:
            from .fetch import FetchRequest
            request_class = FetchRequest
        case ApiKey.API_VERSIONS:
            from .api_versions import ApiVersionsRequest
            request_class = ApiVersionsRequest
        case ApiKey.DESCRIBE_TOPIC_PARTITIONS:
            from .describe_topic_partitions import DescribeTopicPartitionsRequest
            request_class = DescribeTopicPartitionsRequest

    return request_class.decode_body(header, readable)
