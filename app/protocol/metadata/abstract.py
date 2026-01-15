__all__ = "AbstractMetadataRecord",

from abc import ABC, abstractmethod
from typing import BinaryIO, Self


class AbstractMetadataRecord(ABC):
    @classmethod
    @abstractmethod
    def read(cls, stream: BinaryIO) -> Self:
        raise NotImplementedError
