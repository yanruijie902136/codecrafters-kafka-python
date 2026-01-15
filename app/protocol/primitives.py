__all__ = (
    "encode_boolean",
    "encode_compact_array",
    "encode_compact_string",
    "encode_int16",
    "encode_int32",
    "encode_int64",
    "encode_int8",
    "encode_uuid",
    "encode_varint",
    "read_compact_array",
    "read_compact_string",
    "read_int16",
    "read_int32",
    "read_int64",
    "read_int8",
    "read_string",
    "read_uint32",
    "read_uuid",
    "read_varint",
    "read_varlong",
)

import itertools
from typing import BinaryIO, Callable, List, Literal, Optional, Protocol, Sequence, TypeVar, overload
from uuid import UUID

from .utils import readexactly


_T = TypeVar("_T")


def encode_boolean(flag: bool) -> bytes:
    return encode_int8(1 if flag else 0)


def read_int8(stream: BinaryIO) -> int:
    return int.from_bytes(readexactly(stream, 1), byteorder="big", signed=True)


def encode_int8(n: int) -> bytes:
    return n.to_bytes(1, byteorder="big", signed=True)


def read_int16(stream: BinaryIO) -> int:
    return int.from_bytes(readexactly(stream, 2), byteorder="big", signed=True)


def encode_int16(n: int) -> bytes:
    return n.to_bytes(2, byteorder="big", signed=True)


def read_int32(stream: BinaryIO) -> int:
    return int.from_bytes(readexactly(stream, 4), byteorder="big", signed=True)


def encode_int32(n: int) -> bytes:
    return n.to_bytes(4, byteorder="big", signed=True)


def read_int64(stream: BinaryIO) -> int:
    return int.from_bytes(readexactly(stream, 8), byteorder="big", signed=True)


def encode_int64(n: int) -> bytes:
    return n.to_bytes(8, byteorder="big", signed=True)


def read_uint32(stream: BinaryIO) -> int:
    return int.from_bytes(readexactly(stream, 4), byteorder="big", signed=False)


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


def read_varlong(stream: BinaryIO, signed: bool = False) -> int:
    n = 0
    for shamt in itertools.count(start=0, step=7):
        c = ord(readexactly(stream, 1))
        n |= (c & 0x7f) << shamt
        if not c & 0x80:
            break
        elif shamt >= 63:
            raise ValueError("Too many bytes in varlong")

    if signed:
        n = (n >> 1) ^ -(n & 1)
    return n


def read_uuid(stream: BinaryIO) -> UUID:
    return UUID(bytes=readexactly(stream, 16))


def encode_uuid(u: UUID) -> bytes:
    return u.bytes


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


def encode_compact_string(s: Optional[str]) -> bytes:
    if s is None:
        return encode_varint(0)
    return encode_varint(len(s)+1) + s.encode()


@overload
def read_compact_array(stream: BinaryIO, read_func: Callable[[BinaryIO], _T], allow_none: Literal[False] = False) -> List[_T]: ...
@overload
def read_compact_array(stream: BinaryIO, read_func: Callable[[BinaryIO], _T], allow_none: Literal[True]) -> Optional[List[_T]]: ...


def read_compact_array(stream, read_func, allow_none=False):
    length = read_varint(stream) - 1
    if length < 0:
        if allow_none:
            return None
        else:
            raise ValueError("Non-optional compact array has null value")
    return [read_func(stream) for _ in range(length)]


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
