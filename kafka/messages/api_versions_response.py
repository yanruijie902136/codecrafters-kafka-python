import dataclasses

from ..protocol import (
    ApiKey,
    ErrorCode,
    encode_compact_array,
    encode_int16,
    encode_int32,
    encode_tagged_fields,
)

from .abstract_response import AbstractResponse
from .api_versions_request import ApiVersionsRequest


@dataclasses.dataclass
class ApiVersionsResponseApiKey:
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
class ApiVersionsResponse(AbstractResponse):
    error_code: ErrorCode
    api_keys: list[ApiVersionsResponseApiKey]
    throttle_time_ms: int

    @classmethod
    def make_body_kwargs(cls, request: ApiVersionsRequest):
        if request.header.api_version in [0, 1, 2, 3, 4]:
            error_code = ErrorCode.NONE
        else:
            error_code = ErrorCode.UNSUPPORTED_VERSION

        return {
            "error_code": error_code,
            "api_keys": [
                ApiVersionsResponseApiKey(ApiKey.FETCH, min_version=0, max_version=16),
                ApiVersionsResponseApiKey(ApiKey.API_VERSIONS, min_version=0, max_version=4),
                ApiVersionsResponseApiKey(ApiKey.DESCRIBE_TOPIC_PARTITIONS, min_version=0, max_version=0),
            ],
            "throttle_time_ms": 0,
        }

    def _encode_body(self):
        return b"".join([
            self.error_code.encode(),
            encode_compact_array(self.api_keys),
            encode_int32(self.throttle_time_ms),
            encode_tagged_fields(),
        ])
