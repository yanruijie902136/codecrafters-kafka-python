import enum
from typing import BinaryIO

from .primitive_types import decode_int16, encode_int16


@enum.unique
class ApiKey(enum.IntEnum):
    FETCH = 1
    API_VERSIONS = 18
    DESCRIBE_TOPIC_PARTITIONS = 75

    @classmethod
    def decode(cls, binary_stream: BinaryIO):
        return ApiKey(decode_int16(binary_stream))

    def encode(self):
        return encode_int16(self)


@enum.unique
class ErrorCode(enum.IntEnum):
    NONE = 0
    UNKNOWN_TOPIC_OR_PARTITION = 3
    UNSUPPORTED_VERSION = 35
    UNKNOWN_TOPIC_ID = 100

    @classmethod
    def decode(cls, binary_stream: BinaryIO):
        return ErrorCode(decode_int16(binary_stream))

    def encode(self):
        return encode_int16(self)
