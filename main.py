import pprint
import selectors
import socket

import kafka


class KafkaServer:
    def __init__(self, host: str, port: int):
        self.server_socket = socket.create_server((host, port), reuse_port=True)
        self.server_socket.setblocking(False)

        self.selector = selectors.DefaultSelector()
        self.selector.register(self.server_socket, selectors.EVENT_READ)

    def start(self):
        while True:
            for key, _ in self.selector.select():
                if key.fileobj is self.server_socket:
                    self.accept_new_client()
                else:
                    self.serve_client(key.fileobj)

    def accept_new_client(self):
        client_socket, _ = self.server_socket.accept()
        client_socket.setblocking(False)
        self.selector.register(client_socket, selectors.EVENT_READ)

    def serve_client(self, client_socket: socket.socket):
        bytes = client_socket.recv(8192)
        if not bytes:
            self.disconnect_client(client_socket)
            return

        request = kafka.Request.from_bytes(bytes)
        pprint.pprint(request)
        response = kafka.Response.from_request(request)
        pprint.pprint(response)
        client_socket.sendall(response.encode())

    def disconnect_client(self, client_socket: socket.socket):
        self.selector.unregister(client_socket)
        client_socket.close()


if __name__ == "__main__":
    server = KafkaServer(host="localhost", port=9092)
    server.start()
