import abc
import dataclasses

from ..primitive_types import BinaryStream

from .request_header import RequestHeader


@dataclasses.dataclass
class AbstractRequest(abc.ABC):
    header: RequestHeader

    @classmethod
    @abc.abstractmethod
    def decode_body_kwargs(cls, binary_stream: BinaryStream):
        raise NotImplementedError
