import dataclasses
import io

from ..decode_functions import *
from .request import RequestBody


@dataclasses.dataclass
class ApiVersionsRequestBody(RequestBody):
    client_software_name: str
    client_software_version: str

    @staticmethod
    def decode(byte_stream: io.BytesIO):
        body = ApiVersionsRequestBody(
            client_software_name=decode_compact_string(byte_stream),
            client_software_version=decode_compact_string(byte_stream),
        )
        decode_tagged_fields(byte_stream)
        return body
