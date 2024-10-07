from __future__ import annotations

import dataclasses

from ..protocol import ApiKey, encode_int32, encode_tagged_fields

from .request_header import RequestHeader


@dataclasses.dataclass
class ResponseHeader:
    # The `api_key` attribute is only used for deciding the header format. It's
    # not part of the response header, and will not be sent to the clients.
    api_key: ApiKey
    correlation_id: int

    @classmethod
    def from_request_header(cls, request_header: RequestHeader) -> ResponseHeader:
        return ResponseHeader(
            api_key=request_header.api_key,
            correlation_id=request_header.correlation_id,
        )

    def encode(self) -> bytes:
        # The ApiVersions response uses the v0 header format, which doesn't
        # contain TAG_BUFFER. All other responses use the v1 header format.
        if self.api_key is ApiKey.API_VERSIONS:
            return encode_int32(self.correlation_id)
        return encode_int32(self.correlation_id) + encode_tagged_fields()
