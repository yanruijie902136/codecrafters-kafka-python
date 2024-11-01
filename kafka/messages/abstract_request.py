import abc
import dataclasses
from typing import BinaryIO

from .request_header import RequestHeader


@dataclasses.dataclass
class AbstractRequest(abc.ABC):
    header: RequestHeader

    @classmethod
    @abc.abstractmethod
    def decode_body_kwargs(cls, binary_stream: BinaryIO):
        raise NotImplementedError
