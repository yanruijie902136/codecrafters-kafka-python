from __future__ import annotations

import dataclasses
import io

from ..protocol import (
    ApiKey,
    decode_int16,
    decode_int32,
    decode_nullable_string,
    decode_tagged_fields,
)


@dataclasses.dataclass
class RequestHeader:
    api_key: ApiKey
    api_version: int
    correlation_id: int
    client_id: str

    @classmethod
    def decode(cls, byte_stream: io.BufferedIOBase) -> RequestHeader:
        request_header = RequestHeader(
            api_key=ApiKey.decode(byte_stream),
            api_version=decode_int16(byte_stream),
            correlation_id=decode_int32(byte_stream),
            client_id=decode_nullable_string(byte_stream),
        )
        decode_tagged_fields(byte_stream)
        return request_header
