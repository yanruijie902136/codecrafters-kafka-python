__all__ = "AbstractRequest",

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import BinaryIO, Self

from .header import RequestHeader


@dataclass(frozen=True)
class AbstractRequest(ABC):
    header: RequestHeader

    @classmethod
    @abstractmethod
    def read(cls, stream: BinaryIO, header: RequestHeader) -> Self:
        raise NotImplementedError
