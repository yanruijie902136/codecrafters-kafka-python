import asyncio
import pprint

from .protocol import KafkaRequest, KafkaResponse


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

            request = KafkaRequest.from_bytes(data)
            pprint.pprint(request)
            response = KafkaResponse.from_request(request)
            pprint.pprint(response)

            writer.write(response.encode())
            await writer.drain()

        writer.close()
        await writer.wait_closed()
