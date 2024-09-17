import enum

__all__ = [
    "ApiKey",
    "ErrorCode",
]


@enum.unique
class ApiKey(enum.IntEnum):
    FETCH = 1
    API_VERSIONS = 18


@enum.unique
class ErrorCode(enum.IntEnum):
    NONE = 0
    UNSUPPORTED_VERSION = 35
    UNKNOWN_TOPIC = 100
