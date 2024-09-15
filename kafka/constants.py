from enum import IntEnum

__all__ = [
    "ApiKey",
    "ErrorCode",
]


class ApiKey(IntEnum):
    FETCH = 1
    API_VERSIONS = 18


class ErrorCode(IntEnum):
    NONE = 0
    UNSUPPORTED_VERSION = 35
    UNKNOWN_TOPIC = 100
