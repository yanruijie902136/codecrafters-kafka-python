from __future__ import annotations

import enum
import io

from .decode_functions import decode_int16
from .encode_functions import encode_int16


@enum.unique
class ErrorCode(enum.IntEnum):
    NONE = 0
    UNKNOWN_TOPIC_OR_PARTITION = 3
    UNSUPPORTED_VERSION = 35
    UNKNOWN_TOPIC_ID = 100

    @classmethod
    def decode(cls, byte_stream: io.BufferedIOBase) -> ErrorCode:
        return ErrorCode(decode_int16(byte_stream))

    def encode(self) -> bytes:
        return encode_int16(self)
