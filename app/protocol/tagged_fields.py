__all__ = "encode_tagged_fields", "read_tagged_fields"

from typing import BinaryIO

from .utils import readexactly


def read_tagged_fields(stream: BinaryIO) -> None:
    if readexactly(stream, 1) != b"\x00":
        raise ValueError("Unexpected tagged fields")


def encode_tagged_fields() -> bytes:
    return b"\x00"
