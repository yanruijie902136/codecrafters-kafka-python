from abc import ABC, abstractmethod
from dataclasses import dataclass
from socket import socket
from typing import Optional

from .constants import ApiKey
from .primitive_types import *


@dataclass
class RequestHeader:
    api_key: ApiKey
    api_version: int
    correlation_id: int
    client_id: Optional[str]

    @staticmethod
    def decode(bytes: bytes):
        api_key, bytes = decode_int16(bytes)
        api_key = ApiKey(api_key)
        api_version, bytes = decode_int16(bytes)
        correlation_id, bytes = decode_int32(bytes)
        client_id, bytes = decode_nullable_string(bytes)

        header = RequestHeader(
            api_key=api_key,
            api_version=api_version,
            correlation_id=correlation_id,
            client_id=client_id,
        )
        return header, bytes


class RequestBody(ABC):
    @staticmethod
    @abstractmethod
    def decode(bytes: bytes):
        raise NotImplementedError


@dataclass
class Request:
    header: RequestHeader
    body: RequestBody

    @staticmethod
    def from_client(client_socket: socket):
        bytes = client_socket.recv(2048)
        _, bytes = decode_int32(bytes)
        header, bytes = RequestHeader.decode(bytes)
        _, bytes = decode_tagged_fields(bytes)

        match header.api_key:
            case ApiKey.FETCH:
                from .fetch import FetchRequestBody
                body = FetchRequestBody.decode(bytes)
            case ApiKey.API_VERSIONS:
                from .api_versions import ApiVersionsRequestBody
                body = ApiVersionsRequestBody.decode(bytes)

        return Request(header=header, body=body)
