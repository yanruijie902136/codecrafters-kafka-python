import enum


@enum.unique
class ErrorCode(enum.IntEnum):
    NONE = 0
    UNSUPPORTED_VERSION = 35
