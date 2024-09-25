from __future__ import annotations

import abc
import dataclasses

from .api_key import ApiKey
from .encode_functions import encode_int32, encode_tagged_fields
from .request import KafkaRequest, KafkaRequestHeader


@dataclasses.dataclass
class KafkaResponseHeader:
    api_key: ApiKey     # Only used for deciding the header format. This won't be sent.
    correlation_id: int

    @classmethod
    def from_request_header(cls, request_header: KafkaRequestHeader) -> KafkaResponseHeader:
        return KafkaResponseHeader(
            api_key=request_header.api_key,
            correlation_id=request_header.correlation_id,
        )

    def encode(self) -> bytes:
        if self.api_key is ApiKey.API_VERSIONS:
            # The ApiVersions response uses the v0 format (without TAG_BUFFER).
            return encode_int32(self.correlation_id)
        return encode_int32(self.correlation_id) + encode_tagged_fields()


class KafkaResponseBody(abc.ABC):
    @classmethod
    @abc.abstractmethod
    def from_request(cls, request: KafkaRequest) -> KafkaResponseBody:
        raise NotImplementedError

    @abc.abstractmethod
    def encode(self) -> bytes:
        raise NotImplementedError


@dataclasses.dataclass
class KafkaResponse:
    header: KafkaResponseHeader
    body: KafkaResponseBody

    @classmethod
    def from_request(cls, request: KafkaRequest) -> KafkaResponse:
        header = KafkaResponseHeader.from_request_header(request.header)

        body: KafkaResponseBody
        match request.header.api_key:
            case ApiKey.FETCH:
                from .fetch import FetchResponseBody
                body = FetchResponseBody.from_request(request)
            case ApiKey.API_VERSIONS:
                from .api_versions import ApiVersionsResponseBody
                body = ApiVersionsResponseBody.from_request(request)

        return KafkaResponse(header, body)

    def encode(self) -> bytes:
        message = self.header.encode() + self.body.encode()
        return encode_int32(len(message)) + message
