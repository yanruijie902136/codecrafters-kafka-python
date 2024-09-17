import abc
import dataclasses
import io

from ..api_key import ApiKey
from ..decode_functions import *


@dataclasses.dataclass
class RequestHeader:
    api_key: ApiKey
    api_version: int
    correlation_id: int
    client_id: str

    @staticmethod
    def decode(byte_stream: io.BytesIO):
        header = RequestHeader(
            api_key=ApiKey.decode(byte_stream),
            api_version=decode_int16(byte_stream),
            correlation_id=decode_int32(byte_stream),
            client_id=decode_nullable_string(byte_stream),
        )
        decode_tagged_fields(byte_stream)
        return header


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
        decode_int32(byte_stream)   # The first 4 bytes represent the message length.
        header = RequestHeader.decode(byte_stream)

        match header.api_key:
            case ApiKey.FETCH:
                from .fetch import FetchRequestBody
                body = FetchRequestBody.decode(byte_stream)
            case ApiKey.API_VERSIONS:
                from .api_versions import ApiVersionsRequestBody
                body = ApiVersionsRequestBody.decode(byte_stream)

        assert not byte_stream.read(), "Unexpected unused bytes."

        return Request(header=header, body=body)
