import socket
from dataclasses import dataclass
from enum import Enum, unique


@unique
class ErrorCode(Enum):
    NONE = 0
    UNSUPPORTED_VERSION = 35


@dataclass
class KafkaRequest:
    api_key: int
    api_version: int
    correlation_id: int

    @staticmethod
    def from_client(client: socket.socket):
        data = client.recv(2048)
        return KafkaRequest(
            api_key=int.from_bytes(data[4:6]),
            api_version=int.from_bytes(data[6:8]),
            correlation_id=int.from_bytes(data[8:12]),
        )


def make_response(request: KafkaRequest):
    valid_api_versions = [0, 1, 2, 3, 4]
    error_code = (
        ErrorCode.NONE
        if request.api_version in valid_api_versions
        else ErrorCode.UNSUPPORTED_VERSION
    )

    response = (
        int(10).to_bytes(4) +
        request.correlation_id.to_bytes(4) +
        error_code.value.to_bytes(2)
    )
    return response


def main():
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    client, _ = server.accept()
    request = KafkaRequest.from_client(client)
    print(request)
    client.sendall(make_response(request))


if __name__ == "__main__":
    main()
