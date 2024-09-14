import socket


def main():
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    client, _ = server.accept()
    request = client.recv(2048)
    correlation_id_bytes = request[8:12]
    response = bytes(4) + correlation_id_bytes
    client.sendall(response)


if __name__ == "__main__":
    main()
