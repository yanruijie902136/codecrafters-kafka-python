import dataclasses

from ..protocol import ApiKey, encode_int32, encode_tagged_fields

from .request_header import RequestHeader


@dataclasses.dataclass
class ResponseHeader:
    api_key: ApiKey
    correlation_id: int

    @classmethod
    def from_request_header(cls, request_header: RequestHeader):
        return ResponseHeader(
            api_key=request_header.api_key,
            correlation_id=request_header.correlation_id,
        )

    def encode(self):
        if self.api_key is ApiKey.API_VERSIONS:
            return encode_int32(self.correlation_id)
        return encode_int32(self.correlation_id) + encode_tagged_fields()
