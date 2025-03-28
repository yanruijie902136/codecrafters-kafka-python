import dataclasses
from typing import BinaryIO

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
    def decode(cls, binary_stream: BinaryIO):
        request_header = RequestHeader(
            api_key=ApiKey.decode(binary_stream),
            api_version=decode_int16(binary_stream),
            correlation_id=decode_int32(binary_stream),
            client_id=decode_nullable_string(binary_stream),
        )
        decode_tagged_fields(binary_stream)
        return request_header
