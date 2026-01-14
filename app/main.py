import asyncio
import io
from asyncio import StreamReader, StreamWriter

from .handlers import handle_request
from .protocol.requests import AbstractRequest, read_request
from .protocol.responses import AbstractResponse


class Client:
    def __init__(self, reader: StreamReader, writer: StreamWriter) -> None:
        self._reader = reader
        self._writer = writer

    async def recv(self, n: int) -> bytes:
        return await self._reader.readexactly(n)

    async def send(self, data: bytes) -> None:
        self._writer.write(data)
        await self._writer.drain()

    async def close(self) -> None:
        self._writer.close()
        await self._writer.wait_closed()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()
        if exc_value is None:
            return None
        return isinstance(exc_value, asyncio.IncompleteReadError)


async def recv_request_from_client(client: Client) -> AbstractRequest:
    message_size = int.from_bytes(await client.recv(4), byteorder="big")
    data = await client.recv(message_size)
    stream = io.BytesIO(data)
    return read_request(stream)


async def send_response_to_client(client: Client, response: AbstractResponse) -> None:
    encoded_response = response.encode()
    data = len(encoded_response).to_bytes(4, byteorder="big") + encoded_response
    await client.send(data)


async def client_connected_cb(reader: StreamReader, writer: StreamWriter) -> None:
    client = Client(reader, writer)
    async with client:
        while True:
            request = await recv_request_from_client(client)
            response = handle_request(request)
            await send_response_to_client(client, response)


async def main() -> None:
    server = await asyncio.start_server(client_connected_cb, "localhost", 9092)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
