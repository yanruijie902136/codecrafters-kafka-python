__all__ = (
    "handle_api_versions_request",
    "handle_request",
)

from ..protocol.requests import *
from ..protocol.responses import AbstractResponse

from .api_versions import handle_api_versions_request


def handle_request(request: AbstractRequest) -> AbstractResponse:
    if isinstance(request, ApiVersionsRequest):
        return handle_api_versions_request(request)
    else:
        raise TypeError(f"Unsupported request type: {type(request).__name__}")
