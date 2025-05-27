from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Literal, Self

from ..protocol import *
from .request import Request, RequestHeader


@dataclass(frozen=True)
class ResponseHeader:
    correlation_id: int

    def encode(self, version: Literal[0, 1]) -> bytes:
        data = encode_int32(self.correlation_id)
        if version == 1:
            data += encode_tagged_fields()
        return data

    @classmethod
    def from_request_header(cls, request_header: RequestHeader) -> Self:
        return cls(request_header.correlation_id)


@dataclass(frozen=True)
class Response(ABC):
    header: ResponseHeader

    def encode(self) -> bytes:
        return self._encode_header() + self._encode_body()

    def _encode_header(self) -> bytes:
        return self.header.encode(version=1)

    @abstractmethod
    def _encode_body(self) -> bytes:
        raise NotImplementedError


def handle_request(request: Request) -> Response:
    match request.header.request_api_key:
        case ApiKey.FETCH:
            from .fetch import FetchRequest, handle_fetch_request
            request_class, request_handler = FetchRequest, handle_fetch_request
        case ApiKey.API_VERSIONS:
            from .api_versions import ApiVersionsRequest, handle_api_versions_request
            request_class, request_handler = ApiVersionsRequest, handle_api_versions_request
        case ApiKey.DESCRIBE_TOPIC_PARTITIONS:
            from .describe_topic_partitions import DescribeTopicPartitionsRequest, handle_describe_topic_partitions_request
            request_class, request_handler = DescribeTopicPartitionsRequest, handle_describe_topic_partitions_request

    assert isinstance(request, request_class)
    return request_handler(request)
