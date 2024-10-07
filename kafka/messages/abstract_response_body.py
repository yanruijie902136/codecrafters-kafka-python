from __future__ import annotations

import abc

from .request import Request


class AbstractResponseBody(abc.ABC):
    @classmethod
    @abc.abstractmethod
    def from_request(cls, request: Request) -> AbstractResponseBody:
        raise NotImplementedError

    @abc.abstractmethod
    def encode(self) -> bytes:
        raise NotImplementedError
