import socket


def main():
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    client, _ = server.accept()
    request = client.recv(2048)
    client.sendall(b"\x00\x00\x00\x00\x00\x00\x00\x07")


if __name__ == "__main__":
    main()
