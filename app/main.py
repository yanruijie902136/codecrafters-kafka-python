import asyncio
from asyncio import StreamReader, StreamWriter


class Server:
    async def run(self) -> None:
        server = await asyncio.start_server(
            self._client_connected_cb, "localhost", 9092,
            reuse_address=True, reuse_port=True, start_serving=False)

        async with server:
            await server.serve_forever()

    async def _client_connected_cb(self, reader: StreamReader, writer: StreamWriter) -> None:
        raise NotImplementedError


if __name__ == "__main__":
    server = Server()
    asyncio.run(server.run())
