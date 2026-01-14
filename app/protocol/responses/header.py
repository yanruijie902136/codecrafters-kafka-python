__all__ = "ResponseHeader",

from dataclasses import dataclass
from typing import Literal

from ..primitives import encode_int32
from ..tagged_fields import encode_tagged_fields


@dataclass(frozen=True)
class ResponseHeader:
    correlation_id: int

    def encode(self, version: Literal[0, 1]) -> bytes:
        data = encode_int32(self.correlation_id)
        if version == 1:
            data += encode_tagged_fields()
        return data
