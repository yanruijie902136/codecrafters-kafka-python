from __future__ import annotations

import enum
import io

from .primitive_types import decode_int16, encode_int16


@enum.unique
class ApiKey(enum.IntEnum):
    FETCH = 1
    API_VERSIONS = 18
    DESCRIBE_TOPIC_PARTITIONS = 75

    @classmethod
    def decode(cls, byte_stream: io.BufferedIOBase) -> ApiKey:
        return ApiKey(decode_int16(byte_stream))

    def encode(self) -> bytes:
        return encode_int16(self)


@enum.unique
class ErrorCode(enum.IntEnum):
    NONE = 0
    UNKNOWN_TOPIC_OR_PARTITION = 3
    UNSUPPORTED_VERSION = 35
    UNKNOWN_TOPIC_ID = 100

    def encode(self) -> bytes:
        return encode_int16(self)
