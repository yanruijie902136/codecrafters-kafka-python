import dataclasses
import socket

from app.api_key import ApiKey


@dataclasses.dataclass(frozen=True)
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
