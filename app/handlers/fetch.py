__all__ = "handle_fetch_request",

from ..protocol.constants import ErrorCode
from ..protocol.requests import FetchRequest
from ..protocol.responses import FetchResponse

from .utils import make_response_header


def handle_fetch_request(request: FetchRequest) -> FetchResponse:
    return FetchResponse(
        header=make_response_header(request),
        throttle_time_ms=0,
        error_code=ErrorCode.NONE,
        session_id=request.session_id,
        responses=[],
    )
