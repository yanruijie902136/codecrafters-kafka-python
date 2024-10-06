from __future__ import annotations

import abc
import dataclasses

from ..api_key import ApiKey
from ..encode_functions import encode_int32, encode_tagged_fields

from .request import Request, RequestHeader


@dataclasses.dataclass
class ResponseHeader:
    api_key: ApiKey     # Only used for deciding the header format. This won't be sent.
    correlation_id: int

    @classmethod
    def from_request_header(cls, request_header: RequestHeader) -> ResponseHeader:
        return ResponseHeader(
            api_key=request_header.api_key,
            correlation_id=request_header.correlation_id,
        )

    def encode(self) -> bytes:
        if self.api_key is ApiKey.API_VERSIONS:
            # The ApiVersions response uses the v0 format (without TAG_BUFFER).
            return encode_int32(self.correlation_id)
        return encode_int32(self.correlation_id) + encode_tagged_fields()


class AbstractResponseBody(abc.ABC):
    @classmethod
    @abc.abstractmethod
    def from_request(cls, request: Request) -> AbstractResponseBody:
        raise NotImplementedError

    @abc.abstractmethod
    def encode(self) -> bytes:
        raise NotImplementedError


@dataclasses.dataclass
class Response:
    header: ResponseHeader
    body: AbstractResponseBody

    @classmethod
    def from_request(cls, request: Request) -> Response:
        header = ResponseHeader.from_request_header(request.header)

        match request.header.api_key:
            case ApiKey.FETCH:
                from .fetch import FetchResponseBody
                body_class = FetchResponseBody
            case ApiKey.API_VERSIONS:
                from .api_versions import ApiVersionsResponseBody
                body_class = ApiVersionsResponseBody
            case ApiKey.DESCRIBE_TOPIC_PARTITIONS:
                from .describe_topic_partitions import DescribeTopicPartitionsResponseBody
                body_class = DescribeTopicPartitionsResponseBody

        return Response(header, body_class.from_request(request))

    def encode(self) -> bytes:
        message = self.header.encode() + self.body.encode()
        return encode_int32(len(message)) + message
