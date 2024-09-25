from __future__ import annotations

import abc
import dataclasses
import io

from .api_key import ApiKey
from .decode_functions import (
    decode_int16, decode_int32, decode_nullable_string, decode_tagged_fields
)


@dataclasses.dataclass
class KafkaRequestHeader:
    api_key: ApiKey
    api_version: int
    correlation_id: int
    client_id: str

    @classmethod
    def decode(cls, byte_stream: io.BytesIO) -> KafkaRequestHeader:
        header = KafkaRequestHeader(
            api_key=ApiKey.decode(byte_stream),
            api_version=decode_int16(byte_stream),
            correlation_id=decode_int32(byte_stream),
            client_id=decode_nullable_string(byte_stream),
        )
        decode_tagged_fields(byte_stream)
        return header


class KafkaRequestBody(abc.ABC):
    @classmethod
    @abc.abstractmethod
    def decode(cls, byte_stream: io.BytesIO) -> KafkaRequestBody:
        raise NotImplementedError


@dataclasses.dataclass
class KafkaRequest:
    header: KafkaRequestHeader
    body: KafkaRequestBody

    @classmethod
    def from_bytes(cls, data: bytes) -> KafkaRequest:
        byte_stream = io.BytesIO(data)
        decode_int32(byte_stream)   # The first 4 bytes represent the message length.
        header = KafkaRequestHeader.decode(byte_stream)

        body: KafkaRequestBody
        match header.api_key:
            case ApiKey.FETCH:
                from .fetch import FetchRequestBody
                body = FetchRequestBody.decode(byte_stream)
            case ApiKey.API_VERSIONS:
                from .api_versions import ApiVersionsRequestBody
                body = ApiVersionsRequestBody.decode(byte_stream)

        assert not byte_stream.read(), "Unexpected unused bytes."
        return KafkaRequest(header, body)
