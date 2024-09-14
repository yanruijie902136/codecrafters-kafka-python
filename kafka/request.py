from __future__ import annotations

import dataclasses
import socket

from .api_key import ApiKey


@dataclasses.dataclass(frozen=True)
class Request:
    api_key: ApiKey
    api_version: int
    correlation_id: int

    @staticmethod
    def from_client(client_socket: socket.socket) -> Request:
        data = client_socket.recv(1024)
        return Request(
            api_key=ApiKey(int.from_bytes(data[4:6])),
            api_version=int.from_bytes(data[6:8]),
            correlation_id=int.from_bytes(data[8:12]),
        )
