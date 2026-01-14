__all__ = "ApiVersion", "ApiVersionsResponse"

from dataclasses import dataclass
from typing import List

from ..constants import ApiKey, ErrorCode
from ..primitives import encode_compact_array, encode_int16, encode_int32
from ..tagged_fields import encode_tagged_fields

from .abstract import AbstractResponse


@dataclass(frozen=True)
class ApiVersion:
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


@dataclass(frozen=True)
class ApiVersionsResponse(AbstractResponse):
    error_code: ErrorCode
    api_keys: List[ApiVersion]
    throttle_time_ms: int

    def encode(self) -> bytes:
        return b"".join([
            self.header.encode(version=0),
            self.error_code.encode(),
            encode_compact_array(self.api_keys),
            encode_int32(self.throttle_time_ms),
            encode_tagged_fields(),
        ])
