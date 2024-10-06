from __future__ import annotations

import dataclasses

from ...api_key import ApiKey
from ...encode_functions import (
    encode_compact_array,
    encode_int16,
    encode_int32,
    encode_tagged_fields,
)
from ...error_code import ErrorCode

from ..request import Request
from ..response import AbstractResponseBody

from .request_body import ApiVersionsRequestBody


@dataclasses.dataclass
class ApiVersionsResponseBody(AbstractResponseBody):
    error_code: ErrorCode
    api_keys: list[ApiKeyStruct]
    throttle_time_ms: int

    @classmethod
    def from_request(cls, request: Request) -> ApiVersionsResponseBody:
        assert type(request.body) is ApiVersionsRequestBody, "Mismatched request body."

        VALID_API_VERSIONS = [0, 1, 2, 3, 4]
        if request.header.api_version in VALID_API_VERSIONS:
            error_code = ErrorCode.NONE
        else:
            error_code = ErrorCode.UNSUPPORTED_VERSION

        return ApiVersionsResponseBody(
            error_code=error_code,
            api_keys=[
                ApiKeyStruct(api_key=ApiKey.FETCH, min_version=0, max_version=16),
                ApiKeyStruct(api_key=ApiKey.API_VERSIONS, min_version=0, max_version=4),
                ApiKeyStruct(api_key=ApiKey.DESCRIBE_TOPIC_PARTITIONS, min_version=0, max_version=0),
            ],
            throttle_time_ms=0,
        )

    def encode(self) -> bytes:
        return b"".join([
            self.error_code.encode(),
            encode_compact_array(self.api_keys),
            encode_int32(self.throttle_time_ms),
            encode_tagged_fields(),
        ])


@dataclasses.dataclass
class ApiKeyStruct:
    api_key: ApiKey
    min_version: int
    max_version: int

    def encode(self) -> bytes:
        return b"".join([
            self.api_key.encode(),
            encode_int16(self.min_version),
            encode_int16(self.max_version),
            encode_tagged_fields(),
        ])
