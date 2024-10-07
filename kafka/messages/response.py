from __future__ import annotations

import dataclasses

from ..protocol import ApiKey, encode_int32

from .abstract_response_body import AbstractResponseBody
from .request import Request
from .response_header import ResponseHeader


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
