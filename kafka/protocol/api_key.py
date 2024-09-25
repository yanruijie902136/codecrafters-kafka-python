from __future__ import annotations

import enum
import io

from .decode_functions import decode_int16
from .encode_functions import encode_int16


@enum.unique
class ApiKey(enum.IntEnum):
    FETCH = 1
    API_VERSIONS = 18

    @classmethod
    def decode(cls, byte_stream: io.BytesIO) -> ApiKey:
        return ApiKey(decode_int16(byte_stream))

    def encode(self) -> bytes:
        return encode_int16(self)
