import selectors
import socket
from typing import NoReturn

from kafka import Request, request_to_response


class Server:
    def __init__(self, address: tuple[str, int]) -> None:
        self.server_socket = socket.create_server(address, reuse_port=True)
        self.server_socket.setblocking(False)

        self.selector = selectors.DefaultSelector()
        self.selector.register(self.server_socket, selectors.EVENT_READ)

    def start(self) -> NoReturn:
        while True:
            for key, _ in self.selector.select():
                if key.fileobj is self.server_socket:
                    self.accept_new_client()
                else:
                    self.handle_client(client_socket=key.fileobj)

    def accept_new_client(self) -> None:
        client_socket, _ = self.server_socket.accept()
        client_socket.setblocking(False)
        self.selector.register(client_socket, selectors.EVENT_READ)

    def handle_client(self, client_socket: socket.socket) -> None:
        try:
            request = Request.from_client(client_socket)
        except:
            self.selector.unregister(client_socket)
            client_socket.close()
            return
        print(request)
        client_socket.sendall(request_to_response(request))


if __name__ == '__main__':
    Server(('localhost', 9092)).start()
