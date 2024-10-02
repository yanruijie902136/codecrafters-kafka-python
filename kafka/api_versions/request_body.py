from __future__ import annotations

import dataclasses
import io

from ..decode_functions import decode_compact_string, decode_tagged_fields
from ..request import KafkaRequestBody


@dataclasses.dataclass
class ApiVersionsRequestBody(KafkaRequestBody):
    client_software_name: str
    client_software_version: str

    @classmethod
    def decode(cls, byte_stream: io.BytesIO) -> ApiVersionsRequestBody:
        body = ApiVersionsRequestBody(
            client_software_name=decode_compact_string(byte_stream),
            client_software_version=decode_compact_string(byte_stream),
        )
        decode_tagged_fields(byte_stream)
        return body
