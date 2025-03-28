import asyncio

from .messages import make_response, read_request


class KafkaClientConnection:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self._reader = reader
        self._writer = writer

    async def recv_request(self):
        return await read_request(self._reader)

    async def send_response(self, response):
        self._writer.write(response.encode())
        await self._writer.drain()

    async def __aenter__(self):
        pass

    async def __aexit__(self, exc_type, exc, tb):
        self._writer.close()
        await self._writer.wait_closed()


class KafkaServer:
    def __init__(self, host="localhost", port=9092):
        self._host = host
        self._port = port

    async def start(self):
        server = await asyncio.start_server(self._client_connected_cb, self._host, self._port, reuse_port=True)
        async with server:
            await server.serve_forever()

    async def _client_connected_cb(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        conn = KafkaClientConnection(reader, writer)
        async with conn:
            while True:
                request = await conn.recv_request()
                response = make_response(request)
                await conn.send_response(response)
