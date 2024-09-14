import select
import socket
from dataclasses import dataclass
from enum import Enum, unique


@unique
class ApiKey(Enum):
    FETCH = 1
    API_VERSIONS = 18


@unique
class ErrorCode(Enum):
    NONE = 0
    UNSUPPORTED_VERSION = 35


@dataclass
class KafkaRequest:
    api_key: ApiKey
    api_version: int
    correlation_id: int

    @staticmethod
    def from_client(client: socket.socket):
        data = client.recv(2048)
        return KafkaRequest(
            api_key=ApiKey(int.from_bytes(data[4:6])),
            api_version=int.from_bytes(data[6:8]),
            correlation_id=int.from_bytes(data[8:12]),
        )


@dataclass
class ApiKeyEntry:
    api_key: ApiKey
    min_version: int
    max_version: int

    def __bytes__(self):
        return self.api_key.value.to_bytes(2) + self.min_version.to_bytes(2) + self.max_version.to_bytes(2) + b"\x00"


def make_response(request: KafkaRequest):
    response_header = request.correlation_id.to_bytes(4)

    valid_api_versions = [0, 1, 2, 3, 4]
    error_code = (
        ErrorCode.NONE
        if request.api_version in valid_api_versions
        else ErrorCode.UNSUPPORTED_VERSION
    )
    throttle_time_ms = 0
    tag_buffer = b"\x00"
    response_body = (
        error_code.value.to_bytes(2) +
        int(3).to_bytes(1) +
        bytes(ApiKeyEntry(ApiKey.FETCH, min_version=0, max_version=16)) +
        bytes(ApiKeyEntry(ApiKey.API_VERSIONS, min_version=0, max_version=4)) +
        throttle_time_ms.to_bytes(4) +
        tag_buffer
    )

    response_length = len(response_header) + len(response_body)
    return int(response_length).to_bytes(4) + response_header + response_body


def main():
    server_socket = socket.create_server(("localhost", 9092), reuse_port=True)
    client_sockets = set()
    while True:
        ready_read_sockets, _, _ = select.select(
            client_sockets.union({server_socket}), [], []
        )

        for s in ready_read_sockets:
            if s is server_socket:
                client_socket, _ = server_socket.accept()
                client_sockets.add(client_socket)
                continue

            request = KafkaRequest.from_client(s)
            print(request)
            s.sendall(make_response(request))


if __name__ == "__main__":
    main()
