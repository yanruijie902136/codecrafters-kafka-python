import dataclasses

from ..primitive_types import (
    BinaryStream,
    decode_compact_string,
    decode_tagged_fields,
)

from .abstract_request import AbstractRequest


@dataclasses.dataclass
class ApiVersionsRequest(AbstractRequest):
    client_software_name: str
    client_software_version: str

    @classmethod
    def decode_body_kwargs(cls, binary_stream: BinaryStream):
        body_kwargs = {
            "client_software_name": decode_compact_string(binary_stream),
            "client_software_version": decode_compact_string(binary_stream),
        }
        decode_tagged_fields(binary_stream)
        return body_kwargs
