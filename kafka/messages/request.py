from __future__ import annotations

import abc
import asyncio
import dataclasses
import io

from ..protocol import (
    ApiKey,
    decode_int16,
    decode_int32,
    decode_nullable_string,
    decode_tagged_fields,
)


@dataclasses.dataclass
class RequestHeader:
    api_key: ApiKey
    api_version: int
    correlation_id: int
    client_id: str

    @classmethod
    def decode(cls, byte_stream: io.BytesIO) -> RequestHeader:
        header = RequestHeader(
            api_key=ApiKey.decode(byte_stream),
            api_version=decode_int16(byte_stream),
            correlation_id=decode_int32(byte_stream),
            client_id=decode_nullable_string(byte_stream),
        )
        decode_tagged_fields(byte_stream)
        return header


class AbstractRequestBody(abc.ABC):
    @classmethod
    @abc.abstractmethod
    def decode(cls, byte_stream: io.BytesIO) -> AbstractRequestBody:
        raise NotImplementedError


@dataclasses.dataclass
class Request:
    header: RequestHeader
    body: AbstractRequestBody

    @classmethod
    async def from_stream_reader(cls, reader: asyncio.StreamReader) -> Request:
        message_length = int.from_bytes(await reader.readexactly(4))
        byte_stream = io.BytesIO(await reader.readexactly(message_length))

        header = RequestHeader.decode(byte_stream)

        match header.api_key:
            case ApiKey.FETCH:
                from .fetch import FetchRequestBody
                body_class = FetchRequestBody
            case ApiKey.API_VERSIONS:
                from .api_versions import ApiVersionsRequestBody
                body_class = ApiVersionsRequestBody
            case ApiKey.DESCRIBE_TOPIC_PARTITIONS:
                from .describe_topic_partitions import DescribeTopicPartitionsRequestBody
                body_class = DescribeTopicPartitionsRequestBody

        return Request(header, body_class.decode(byte_stream))
