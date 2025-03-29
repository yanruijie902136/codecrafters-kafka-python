import abc
import dataclasses
import typing

from ..protocol import ApiKey, encode_int32

from .request import Request, RequestHeader


@dataclasses.dataclass(frozen=True)
class ResponseHeader:
    correlation_id: int

    def encode(self) -> bytes:
        return encode_int32(self.correlation_id)

    @classmethod
    def from_request_header(cls, request_header: RequestHeader) -> typing.Self:
        return cls(request_header.correlation_id)


@dataclasses.dataclass(frozen=True)
class Response(abc.ABC):
    header: ResponseHeader

    def encode(self) -> bytes:
        return self.header.encode() + self._encode_body()

    @abc.abstractmethod
    def _encode_body(self) -> bytes:
        raise NotImplementedError


def handle_request(request: Request) -> Response:
    match request.header.request_api_key:
        case ApiKey.API_VERSIONS:
            from .api_versions import ApiVersionsRequest, handle_api_versions_request
            request_class, request_handler = ApiVersionsRequest, handle_api_versions_request

    assert isinstance(request, request_class)
    return request_handler(request)
