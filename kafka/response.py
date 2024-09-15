from abc import ABC, abstractmethod
from dataclasses import dataclass

from .constants import ApiKey
from .primitive_types import *
from .request import Request


@dataclass
class ResponseHeader:
    correlation_id: int
    has_tag_buffer: bool

    @staticmethod
    def from_request(request: Request):
        return ResponseHeader(
            correlation_id=request.header.correlation_id,
            has_tag_buffer=request.header.api_key is not ApiKey.API_VERSIONS,
        )

    def encode(self):
        encoding = encode_int32(self.correlation_id)
        if self.has_tag_buffer:
            encoding += encode_tagged_fields()
        return encoding


class ResponseBody(ABC):
    @staticmethod
    @abstractmethod
    def from_request(request: Request):
        raise NotImplementedError

    @abstractmethod
    def encode(self) -> bytes:
        raise NotImplementedError


@dataclass
class Response:
    header: ResponseHeader
    body: ResponseBody

    @staticmethod
    def from_request(request: Request):
        header = ResponseHeader.from_request(request)
        match request.header.api_key:
            case ApiKey.FETCH:
                from .fetch import FetchResponseBody
                body = FetchResponseBody.from_request(request)
            case ApiKey.API_VERSIONS:
                from .api_versions import ApiVersionsResponseBody
                body = ApiVersionsResponseBody.from_request(request)

        return Response(header=header, body=body)

    def encode(self):
        encoding = self.header.encode() + self.body.encode()
        return encode_int32(len(encoding)) + encoding
