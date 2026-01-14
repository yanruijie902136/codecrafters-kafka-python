__all__ = "ApiVersionsRequest",

from dataclasses import dataclass
from typing import BinaryIO, Self

from ..primitives import read_compact_string
from ..tagged_fields import read_tagged_fields

from .abstract import AbstractRequest
from .header import RequestHeader


@dataclass(frozen=True)
class ApiVersionsRequest(AbstractRequest):
    client_software_name: str
    client_software_version: str

    @classmethod
    def read(cls, stream: BinaryIO, header: RequestHeader) -> Self:
        request = cls(
            header=header,
            client_software_name=read_compact_string(stream),
            client_software_version=read_compact_string(stream),
        )
        read_tagged_fields(stream)
        return request
