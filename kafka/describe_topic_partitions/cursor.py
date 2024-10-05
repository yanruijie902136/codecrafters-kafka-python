from __future__ import annotations

import dataclasses
import io


@dataclasses.dataclass  # FIXME: Only allow null cursor (a 0xff byte) for now.
class Cursor:
    @classmethod
    def decode(cls, byte_stream: io.BytesIO) -> Cursor:
        assert byte_stream.read(1) == b"\xff", "Cursor should be null."
        return Cursor()

    def encode(self) -> bytes:
        return b"\xff"
