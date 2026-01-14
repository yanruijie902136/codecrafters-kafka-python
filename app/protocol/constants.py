__all__ = "ApiKey", "ErrorCode"

from enum import IntEnum, unique
from typing import BinaryIO, Self

from .primitives import encode_int16, read_int16


@unique
class ApiKey(IntEnum):
    API_VERSIONS = 18
    DESCRIBE_TOPIC_PARTITIONS = 75

    @classmethod
    def read(cls, stream: BinaryIO) -> Self:
        return cls(read_int16(stream))

    def encode(self) -> bytes:
        return encode_int16(self)


@unique
class ErrorCode(IntEnum):
    NONE = 0
    UNKNOWN_TOPIC_OR_PARTITION = 3
    UNSUPPORTED_VERSION = 35

    def encode(self) -> bytes:
        return encode_int16(self)
