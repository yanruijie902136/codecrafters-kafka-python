import dataclasses
import io

from .constants import ApiKey, ErrorCode
from .primitive_types import *
from .request import Request, RequestBody
from .response import ResponseBody

###########
# Request #
###########


@dataclasses.dataclass
class ApiKeyEntry:
    api_key: ApiKey
    min_version: int
    max_version: int

    def encode(self):
        return b"".join([
            encode_int16(self.api_key),
            encode_int16(self.min_version),
            encode_int16(self.max_version),
            encode_tagged_fields(),
        ])


@dataclasses.dataclass
class ApiVersionsRequestBody(RequestBody):
    client_software_name: str
    client_software_version: str

    @staticmethod
    def decode(byte_stream: io.BytesIO):
        client_software_name = decode_compact_string(byte_stream)
        client_software_version = decode_compact_string(byte_stream)
        decode_tagged_fields(byte_stream)

        return ApiVersionsRequestBody(
            client_software_name=client_software_name,
            client_software_version=client_software_version,
        )


############
# Response #
############


@dataclasses.dataclass
class ApiVersionsResponseBody(ResponseBody):
    error_code: ErrorCode
    api_keys: list[ApiKeyEntry]
    throttle_time_ms: int

    @staticmethod
    def from_request(request: Request):
        assert type(request.body) is ApiVersionsRequestBody, "Wrong request body!"

        valid_api_versions = [0, 1, 2, 3, 4]
        if request.header.api_version not in valid_api_versions:
            error_code = ErrorCode.UNSUPPORTED_VERSION
        else:
            error_code = ErrorCode.NONE

        return ApiVersionsResponseBody(
            error_code=error_code,
            api_keys=[
                ApiKeyEntry(api_key=ApiKey.FETCH, min_version=0, max_version=16),
                ApiKeyEntry(ApiKey.API_VERSIONS, min_version=0, max_version=4),
            ],
            throttle_time_ms=0,
        )

    def encode(self):
        return b"".join([
            encode_int16(self.error_code),
            encode_compact_array(self.api_keys),
            encode_int32(self.throttle_time_ms),
            encode_tagged_fields(),
        ])
