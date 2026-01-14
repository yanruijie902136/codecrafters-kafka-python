__all__ = (
    "encode_compact_array",
    "encode_int16",
    "encode_int32",
    "encode_varint",
    "read_compact_string",
    "read_int16",
    "read_int32",
    "read_string",
    "read_varint",
)


import itertools
from typing import BinaryIO, Callable, Literal, Optional, Protocol, Sequence, TypeVar, overload

from .utils import readexactly


_T = TypeVar("_T")


def read_int16(stream: BinaryIO) -> int:
    return int.from_bytes(readexactly(stream, 2), byteorder="big", signed=True)


def encode_int16(n: int) -> bytes:
    return n.to_bytes(2, byteorder="big", signed=True)


def read_int32(stream: BinaryIO) -> int:
    return int.from_bytes(readexactly(stream, 4), byteorder="big", signed=True)


def encode_int32(n: int) -> bytes:
    return n.to_bytes(4, byteorder="big", signed=True)


def read_varint(stream: BinaryIO, signed: bool = False) -> int:
    n = 0
    for shamt in itertools.count(start=0, step=7):
        c = ord(readexactly(stream, 1))
        n |= (c & 0x7f) << shamt
        if not c & 0x80:
            break
        elif shamt >= 28:
            raise ValueError("Too many bytes in varint")

    if signed:
        n = (n >> 1) ^ -(n & 1)
    return n


def encode_varint(n: int, signed: bool = False) -> bytes:
    val = (n << 1) ^ (n >> 31) if signed else n
    if not 0 <= val < (1 << 32):
        raise OverflowError(f"{n} is out or range for varint")

    data = bytearray()
    while (val & 0xffffff80) != 0:
        data.append((val & 0x7f) | 0x80)
        val >>= 7
    data.append(val)
    return bytes(data)


@overload
def read_string(stream: BinaryIO, allow_none: Literal[False] = False) -> str: ...
@overload
def read_string(stream: BinaryIO, allow_none: Literal[True]) -> Optional[str]: ...


def read_string(stream, allow_none=False):
    length = read_int16(stream)
    if length < 0:
        if allow_none:
            return None
        else:
            raise ValueError("Non-optional string has null value")
    return readexactly(stream, length).decode()


@overload
def read_compact_string(stream: BinaryIO, allow_none: Literal[False] = False) -> str: ...
@overload
def read_compact_string(stream: BinaryIO, allow_none: Literal[True]) -> Optional[str]: ...


def read_compact_string(stream, allow_none=False):
    length = read_varint(stream) - 1
    if length < 0:
        if allow_none:
            return None
        else:
            raise ValueError("Non-optional compact string has null value")
    return readexactly(stream, length).decode()


class _Encodable(Protocol):
    def encode(self) -> bytes: ...


@overload
def encode_compact_array(arr: Optional[Sequence[_Encodable]]) -> bytes: ...
@overload
def encode_compact_array(arr: Optional[Sequence[_T]], encode_func: Callable[[_T], bytes]) -> bytes: ...


def encode_compact_array(arr, encode_func=None):
    if arr is None:
        return encode_varint(0)
    if encode_func is None:
        def encode_func(i: _Encodable) -> bytes:
            return i.encode()
    return encode_varint(len(arr)+1) + b"".join(encode_func(i) for i in arr)
