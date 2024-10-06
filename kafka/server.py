from __future__ import annotations

import asyncio

from .messages import Request, Response


class KafkaServer:
    async def start(self) -> None:
        server = await asyncio.start_server(self._handle_client, "localhost", 9092, reuse_port=True)
        async with server:
            await server.serve_forever()

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        while True:
            try:
                request = await Request.from_stream_reader(reader)
            except asyncio.IncompleteReadError:
                break

            response = Response.from_request(request)
            writer.write(response.encode())
            await writer.drain()

        writer.close()
        await writer.wait_closed()
