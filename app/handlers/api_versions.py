__all__ = "handle_api_versions_request",

from ..protocol.constants import ApiKey, ErrorCode
from ..protocol.requests import ApiVersionsRequest
from ..protocol.responses import ApiVersionsResponse
from ..protocol.responses.api_versions import ApiVersion

from .utils import make_response_header


def handle_api_versions_request(request: ApiVersionsRequest) -> ApiVersionsResponse:
    if request.header.api_version == 4:
        error_code = ErrorCode.NONE
    else:
        error_code = ErrorCode.UNSUPPORTED_VERSION

    return ApiVersionsResponse(
        header=make_response_header(request),
        error_code=error_code,
        api_keys=[
            ApiVersion(ApiKey.FETCH, min_version=16, max_version=16),
            ApiVersion(ApiKey.API_VERSIONS, min_version=4, max_version=4),
            ApiVersion(ApiKey.DESCRIBE_TOPIC_PARTITIONS, min_version=0, max_version=0),
        ],
        throttle_time_ms=0,
    )
