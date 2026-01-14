__all__ = "make_response_header",

from ..protocol.requests import AbstractRequest
from ..protocol.responses import ResponseHeader


def make_response_header(request: AbstractRequest) -> ResponseHeader:
    return ResponseHeader(request.header.correlation_id)
