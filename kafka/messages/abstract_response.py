import abc
import dataclasses

from ..protocol import encode_int32

from .response_header import ResponseHeader


@dataclasses.dataclass
class AbstractResponse(abc.ABC):
    header: ResponseHeader

    @classmethod
    @abc.abstractmethod
    def make_body_kwargs(cls, request):
        raise NotImplementedError

    def encode(self):
        encoded_response = self.header.encode() + self._encode_body()
        return encode_int32(len(encoded_response)) + encoded_response

    @abc.abstractmethod
    def _encode_body(self):
        raise NotImplementedError
