import asyncio


class KafkaClientConnection:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self._reader = reader
        self._writer = writer

    async def __aenter__(self) -> None:
        pass

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        self._writer.close()
        await self._writer.wait_closed()


class KafkaServer:
    async def start(self) -> None:
        server = await asyncio.start_server(self._client_connected_cb, host="localhost", port=9092, reuse_port=True)
        async with server:
            await server.serve_forever()

    async def _client_connected_cb(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        connection = KafkaClientConnection(reader, writer)
        async with connection:
            pass


if __name__ == "__main__":
    asyncio.run(KafkaServer().start())
