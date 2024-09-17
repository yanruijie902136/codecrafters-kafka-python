import abc
import dataclasses
import io

from .constants import ApiKey
from .primitive_types import *


@dataclasses.dataclass
class RequestHeader:
    api_key: ApiKey
    api_version: int
    correlation_id: int
    client_id: str

    @staticmethod
    def decode(byte_stream: io.BytesIO):
        api_key = ApiKey(decode_int16(byte_stream))
        api_version = decode_int16(byte_stream)
        correlation_id = decode_int32(byte_stream)
        client_id = decode_nullable_string(byte_stream)

        return RequestHeader(
            api_key=api_key,
            api_version=api_version,
            correlation_id=correlation_id,
            client_id=client_id,
        )


class RequestBody(abc.ABC):
    @staticmethod
    @abc.abstractmethod
    def decode(byte_stream: io.BytesIO):
        raise NotImplementedError


@dataclasses.dataclass
class Request:
    header: RequestHeader
    body: RequestBody

    @staticmethod
    def from_bytes(bytes: bytes):
        byte_stream = io.BytesIO(bytes)
        _ = decode_int32(byte_stream)
        header = RequestHeader.decode(byte_stream)
        decode_tagged_fields(byte_stream)

        match header.api_key:
            case ApiKey.FETCH:
                from .fetch import FetchRequestBody
                body = FetchRequestBody.decode(byte_stream)
            case ApiKey.API_VERSIONS:
                from .api_versions import ApiVersionsRequestBody
                body = ApiVersionsRequestBody.decode(byte_stream)

        return Request(header=header, body=body)
