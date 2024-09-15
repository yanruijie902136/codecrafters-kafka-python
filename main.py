import pprint
from selectors import EVENT_READ, DefaultSelector
from socket import create_server, socket

from kafka.request import Request
from kafka.response import Response


class KafkaServer:
    def __init__(self, host: str, port: int):
        self.server_socket = create_server((host, port), reuse_port=True)
        self.server_socket.setblocking(False)

        self.selector = DefaultSelector()
        self.selector.register(self.server_socket, EVENT_READ)

    def start(self):
        while True:
            for key, _ in self.selector.select():
                if key.fileobj is self.server_socket:
                    self.accept_new_client()
                else:
                    self.serve_client(client_socket=key.fileobj)

    def accept_new_client(self):
        client_socket, _ = self.server_socket.accept()
        client_socket.setblocking(False)
        self.selector.register(client_socket, EVENT_READ)

    def serve_client(self, client_socket: socket):
        # try:
        #     request = Request.from_client(client_socket)
        # except:
        #     self.selector.unregister(client_socket)
        #     client_socket.close()
        #     return

        request = Request.from_client(client_socket)
        pprint.pprint(request)
        response = Response.from_request(request)
        pprint.pprint(response)
        client_socket.sendall(response.encode())


if __name__ == "__main__":
    server = KafkaServer(host="localhost", port=9092)
    server.start()
