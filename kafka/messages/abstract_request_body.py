from __future__ import annotations

import abc
import io


class AbstractRequestBody(abc.ABC):
    @classmethod
    @abc.abstractmethod
    def decode(cls, byte_stream: io.BufferedIOBase) -> AbstractRequestBody:
        raise NotImplementedError
