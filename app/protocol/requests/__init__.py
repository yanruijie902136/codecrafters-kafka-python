__all__ = (
    "AbstractRequest",
    "ApiVersionsRequest",
    "RequestHeader",
    "read_request",
)

from typing import BinaryIO

from ..constants import ApiKey

from .abstract import AbstractRequest
from .api_versions import ApiVersionsRequest
from .header import RequestHeader


def read_request(stream: BinaryIO) -> AbstractRequest:
    header = RequestHeader.read(stream)
    match header.api_key:
        case ApiKey.API_VERSIONS:
            return ApiVersionsRequest.read(stream, header)
