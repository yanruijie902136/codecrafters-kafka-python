import abc
import dataclasses

from ..api_key import ApiKey
from ..encode_functions import *
from ..request import Request, RequestHeader


@dataclasses.dataclass
class ResponseHeader:
    api_key: ApiKey
    correlation_id: int

    @staticmethod
    def from_request_header(request_header: RequestHeader):
        return ResponseHeader(
            api_key=request_header.api_key,
            correlation_id=request_header.correlation_id,
        )

    def encode(self):
        if self.api_key is ApiKey.API_VERSIONS:
            # The ApiVersions response uses the v0 format (without TAG_BUFFER).
            return encode_int32(self.correlation_id)
        return encode_int32(self.correlation_id) + encode_tagged_fields()


class ResponseBody(abc.ABC):
    @staticmethod
    @abc.abstractmethod
    def from_request(request: Request):
        raise NotImplementedError

    @abc.abstractmethod
    def encode(self) -> bytes:
        raise NotImplementedError


@dataclasses.dataclass
class Response:
    header: ResponseHeader
    body: ResponseBody

    @staticmethod
    def from_request(request: Request):
        header = ResponseHeader.from_request_header(request.header)

        match request.header.api_key:
            case ApiKey.FETCH:
                from .fetch import FetchResponseBody
                body = FetchResponseBody.from_request(request)
            case ApiKey.API_VERSIONS:
                from .api_versions import ApiVersionsResponseBody
                body = ApiVersionsResponseBody.from_request(request)

        return Response(header=header, body=body)

    def encode(self):
        message = self.header.encode() + self.body.encode()
        return encode_int32(len(message)) + message
