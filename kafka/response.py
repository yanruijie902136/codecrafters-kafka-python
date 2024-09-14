from __future__ import annotations

import abc
import dataclasses

from .api_key import ApiKey
from .error_code import ErrorCode
from .request import Request

__all__ = ["Response", "FetchResponse", "ApiVersionsResponse"]

TAG_BUFFER = b"\x00"


class Response(abc.ABC):
    @abc.abstractmethod
    def serialize(self) -> bytes:
        raise NotImplementedError

    @staticmethod
    @abc.abstractmethod
    def from_request(request: Request) -> Response:
        raise NotImplementedError


@dataclasses.dataclass(frozen=True)
class FetchResponse(Response):
    throttle_time_ms: int
    error_code: ErrorCode
    session_id: int
    responses = []

    def serialize(self) -> bytes:
        return b"".join([
            self.throttle_time_ms.to_bytes(4),
            self.error_code.to_bytes(2),
            self.session_id.to_bytes(4),
            TAG_BUFFER,
            int(len(self.responses) + 1).to_bytes(1),
            TAG_BUFFER,
        ])

    @staticmethod
    def from_request(request: Request) -> FetchResponse:
        return FetchResponse(
            throttle_time_ms=0, error_code=ErrorCode.NONE, session_id=0
        )


@dataclasses.dataclass
class ApiKeyEntry:
    api_key: ApiKey
    min_version: int
    max_version: int

    def serialize(self) -> bytes:
        return b"".join([
            self.api_key.to_bytes(2),
            self.min_version.to_bytes(2),
            self.max_version.to_bytes(2),
            TAG_BUFFER,
        ])


@dataclasses.dataclass
class ApiVersionsResponse(Response):
    error_code: ErrorCode
    throttle_time_ms: int
    api_keys = [
        ApiKeyEntry(ApiKey.FETCH, min_version=0, max_version=16),
        ApiKeyEntry(ApiKey.API_VERSIONS, min_version=0, max_version=4),
    ]

    def serialize(self) -> bytes:
        return b"".join([
            self.error_code.to_bytes(2),
            int(len(self.api_keys) + 1).to_bytes(1),
            b"".join(entry.serialize() for entry in self.api_keys),
            self.throttle_time_ms.to_bytes(4),
            TAG_BUFFER,
        ])

    @staticmethod
    def from_request(request: Request) -> ApiVersionsResponse:
        valid_api_versions = [0, 1, 2, 3, 4]
        error_code = (
            ErrorCode.NONE
            if request.api_version in valid_api_versions
            else ErrorCode.UNSUPPORTED_VERSION
        )
        return ApiVersionsResponse(error_code=error_code, throttle_time_ms=0)
