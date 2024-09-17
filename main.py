import pprint
import selectors
import socket

import kafka


class KafkaServer:
    def __init__(self, host: str, port: int):
        self._server_socket = socket.create_server((host, port), reuse_port=True)
        self._server_socket.setblocking(False)

        self._selector = selectors.DefaultSelector()
        self._selector.register(self._server_socket, selectors.EVENT_READ)

    def start(self):
        while True:
            for key, _ in self._selector.select():
                if key.fileobj is self._server_socket:
                    self._accept_new_client()
                else:
                    self._handle_client(key.fileobj)

    def _accept_new_client(self):
        client_socket, _ = self._server_socket.accept()
        client_socket.setblocking(False)
        self._selector.register(client_socket, selectors.EVENT_READ)

    def _handle_client(self, client_socket: socket.socket):
        bytes = client_socket.recv(8192)
        if not bytes:
            self._disconnect_client(client_socket)
            return

        request = kafka.Request.from_bytes(bytes)
        pprint.pprint(request)
        response = kafka.Response.from_request(request)
        pprint.pprint(response)
        client_socket.sendall(response.encode())

    def _disconnect_client(self, client_socket: socket.socket):
        self._selector.unregister(client_socket)
        client_socket.close()


if __name__ == "__main__":
    server = KafkaServer(host="localhost", port=9092)
    server.start()
