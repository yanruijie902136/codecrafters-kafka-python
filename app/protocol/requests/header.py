__all__ = "RequestHeader",

from dataclasses import dataclass
from typing import BinaryIO, Optional, Self

from ..constants import ApiKey
from ..primitives import read_int16, read_int32, read_string
from ..tagged_fields import read_tagged_fields


@dataclass(frozen=True)
class RequestHeader:
    api_key: ApiKey
    api_version: int
    correlation_id: int
    client_id: Optional[str]

    @classmethod
    def read(cls, stream: BinaryIO) -> Self:
        header = cls(
            api_key=ApiKey.read(stream),
            api_version=read_int16(stream),
            correlation_id=read_int32(stream),
            client_id=read_string(stream, allow_none=True),
        )
        read_tagged_fields(stream)
        return header
