__all__ = "AbstractResponse",

from abc import ABC, abstractmethod
from dataclasses import dataclass

from .header import ResponseHeader


@dataclass(frozen=True)
class AbstractResponse(ABC):
    header: ResponseHeader

    @abstractmethod
    def encode(self) -> bytes:
        raise NotImplementedError
