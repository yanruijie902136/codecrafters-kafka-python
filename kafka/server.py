import asyncio

from .messages import make_response, read_request


class KafkaServer:
    def __init__(self, host="localhost", port=9092):
        self._host = host
        self._port = port

    async def start(self):
        server = await asyncio.start_server(self._client_connected_cb, self._host, self._port, reuse_port=True)
        async with server:
            await server.serve_forever()

    async def _client_connected_cb(self, stream_reader: asyncio.StreamReader, stream_writer: asyncio.StreamWriter):
        try:
            while True:
                request = await read_request(stream_reader)
                response = make_response(request)
                stream_writer.write(response.encode())
                await stream_writer.drain()
        except asyncio.IncompleteReadError:
            return
        finally:
            stream_writer.close()
            await stream_writer.wait_closed()
