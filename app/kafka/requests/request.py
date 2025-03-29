import abc
import dataclasses
import typing

from ..protocol import ApiKey, Readable, decode_int16, decode_int32, decode_nullable_string, decode_tagged_fields


@dataclasses.dataclass(frozen=True)
class RequestHeader:
    request_api_key: ApiKey
    request_api_version: int
    correlation_id: int
    client_id: str | None

    @classmethod
    def decode(cls, readable: Readable) -> typing.Self:
        request_header = cls(
            request_api_key=ApiKey.decode(readable),
            request_api_version=decode_int16(readable),
            correlation_id=decode_int32(readable),
            client_id=decode_nullable_string(readable),
        )
        decode_tagged_fields(readable)
        return request_header


@dataclasses.dataclass(frozen=True)
class Request(abc.ABC):
    header: RequestHeader

    @classmethod
    @abc.abstractmethod
    def decode_body(cls, header: RequestHeader, readable: Readable) -> typing.Self:
        raise NotImplementedError


def decode_request(readable: Readable) -> Request:
    header = RequestHeader.decode(readable)
    match header.request_api_key:
        case ApiKey.API_VERSIONS:
            from .api_versions import ApiVersionsRequest
            request_class = ApiVersionsRequest
    return request_class.decode_body(header, readable)
