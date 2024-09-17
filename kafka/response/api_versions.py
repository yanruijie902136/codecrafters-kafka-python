import dataclasses

from ..api_key import ApiKey
from ..encode_functions import *
from ..error_code import ErrorCode
from ..request import ApiVersionsRequestBody, Request
from .response import ResponseBody


@dataclasses.dataclass
class ApiKeyItem:
    api_key: ApiKey
    min_version: int
    max_version: int

    def encode(self):
        return b"".join([
            self.api_key.encode(),
            encode_int16(self.min_version),
            encode_int16(self.max_version),
            encode_tagged_fields(),
        ])


@dataclasses.dataclass
class ApiVersionsResponseBody(ResponseBody):
    error_code: ErrorCode
    api_keys: list[ApiKeyItem]
    throttle_time_ms: int

    @staticmethod
    def from_request(request: Request):
        assert type(request.body) is ApiVersionsRequestBody, "Mismatched request body."

        VALID_API_VERSIONS = [0, 1, 2, 3, 4]
        if request.header.api_version in VALID_API_VERSIONS:
            error_code = ErrorCode.NONE
        else:
            error_code = ErrorCode.UNSUPPORTED_VERSION

        API_KEYS = [
            ApiKeyItem(api_key=ApiKey.FETCH, min_version=0, max_version=16),
            ApiKeyItem(api_key=ApiKey.API_VERSIONS, min_version=0, max_version=4),
        ]

        return ApiVersionsResponseBody(
            error_code=error_code,
            api_keys=API_KEYS,
            throttle_time_ms=0,
        )

    def encode(self):
        return b"".join([
            self.error_code.encode(),
            encode_compact_array(self.api_keys),
            encode_int32(self.throttle_time_ms),
            encode_tagged_fields(),
        ])
