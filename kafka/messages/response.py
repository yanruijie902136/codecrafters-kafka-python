from __future__ import annotations

import dataclasses

from ..constants import ApiKey
from ..primitive_types import encode_int32

from .abstract_response_body import AbstractResponseBody
from .api_versions_response_body import ApiVersionsResponseBody
from .describe_topic_partitions_response_body import DescribeTopicPartitionsResponseBody
from .fetch_response_body import FetchResponseBody
from .request import Request
from .response_header import ResponseHeader


@dataclasses.dataclass
class Response:
    header: ResponseHeader
    body: AbstractResponseBody

    @classmethod
    def from_request(cls, request: Request) -> Response:
        response_header = ResponseHeader.from_request_header(request.header)

        match request.header.api_key:
            case ApiKey.FETCH:
                response_body_class = FetchResponseBody
            case ApiKey.API_VERSIONS:
                response_body_class = ApiVersionsResponseBody
            case ApiKey.DESCRIBE_TOPIC_PARTITIONS:
                response_body_class = DescribeTopicPartitionsResponseBody

        return Response(response_header, response_body_class.from_request(request))

    def encode(self) -> bytes:
        message = self.header.encode() + self.body.encode()
        return encode_int32(len(message)) + message
