from __future__ import annotations

import asyncio
import dataclasses
import io

from ..constants import ApiKey

from .abstract_request_body import AbstractRequestBody
from .api_versions_request_body import ApiVersionsRequestBody
from .describe_topic_partitions_request_body import DescribeTopicPartitionsRequestBody
from .fetch_request_body import FetchRequestBody
from .request_header import RequestHeader


@dataclasses.dataclass
class Request:
    header: RequestHeader
    body: AbstractRequestBody

    @classmethod
    async def from_stream_reader(cls, stream_reader: asyncio.StreamReader) -> Request:
        message_length = int.from_bytes(await stream_reader.readexactly(4))
        byte_stream = io.BytesIO(await stream_reader.readexactly(message_length))

        request_header = RequestHeader.decode(byte_stream)

        match request_header.api_key:
            case ApiKey.FETCH:
                request_body_class = FetchRequestBody
            case ApiKey.API_VERSIONS:
                request_body_class = ApiVersionsRequestBody
            case ApiKey.DESCRIBE_TOPIC_PARTITIONS:
                request_body_class = DescribeTopicPartitionsRequestBody

        return Request(request_header, request_body_class.decode(byte_stream))
