__all__ = "readexactly",

from typing import BinaryIO


def readexactly(stream: BinaryIO, n: int) -> bytes:
    data = stream.read(n)
    if len(data) < n:
        raise ValueError("Not enough bytes")
    return data
