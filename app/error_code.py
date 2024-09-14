import enum


@enum.unique
class ErrorCode(enum.Enum):
    NONE = 0
    UNSUPPORTED_VERSION = 35
