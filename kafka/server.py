import asyncio

from .request import Request
from .response import Response


class KafkaServer:
    def __init__(self, host="localhost", port=9092) -> None:
        self._host = host
        self._port = port

    async def start(self) -> None:
        server = await asyncio.start_server(
            self._handle_client, self._host, self._port, reuse_port=True
        )
        async with server:
            await server.serve_forever()

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        while True:
            data = await reader.read(8192)
            if not data:
                break
            response = Response.from_request(Request.from_bytes(data))
            writer.write(response.encode())
            await writer.drain()
        writer.close()
        await writer.wait_closed()
