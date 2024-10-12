import asyncio

from .messages import Request, Response


class Server:
    async def start(self) -> None:
        server = await asyncio.start_server(self._client_connected_cb, "localhost", 9092, reuse_port=True)
        async with server:
            await server.serve_forever()

    async def _client_connected_cb(self, stream_reader: asyncio.StreamReader, stream_writer: asyncio.StreamWriter) -> None:
        while True:
            try:
                request = await Request.from_stream_reader(stream_reader)
            except asyncio.IncompleteReadError:
                break
            response = Response.from_request(request)
            stream_writer.write(response.encode())
            await stream_writer.drain()

        stream_writer.close()
        await stream_writer.wait_closed()
