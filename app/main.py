import asyncio
from asyncio import StreamReader, StreamWriter
from io import BytesIO
from typing import Self

from .kafka.requests import Request, Response, decode_request, handle_request


class KafkaClientConnection:
    def __init__(self, reader: StreamReader, writer: StreamWriter) -> None:
        self._reader = reader
        self._writer = writer

    async def recv_request(self) -> Request:
        message_size = int.from_bytes(await self._reader.readexactly(4))
        readable = BytesIO(await self._reader.readexactly(message_size))
        return decode_request(readable)

    async def send_response(self, response: Response) -> None:
        data = response.encode()
        self._writer.write(len(data).to_bytes(4) + data)
        await self._writer.drain()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        self._writer.close()
        await self._writer.wait_closed()


class KafkaServer:
    async def start(self) -> None:
        server = await asyncio.start_server(self._client_connected_cb, host="localhost", port=9092, reuse_port=True)
        async with server:
            await server.serve_forever()

    async def _client_connected_cb(self, reader: StreamReader, writer: StreamWriter) -> None:
        async with KafkaClientConnection(reader, writer) as connection:
            while True:
                request = await connection.recv_request()
                response = handle_request(request)
                await connection.send_response(response)


if __name__ == "__main__":
    asyncio.run(KafkaServer().start())
