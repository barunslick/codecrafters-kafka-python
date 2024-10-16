import socket  # noqa: F401


def main():
    server = socket.create_server(("localhost", 9092), reuse_port=True)

    client, _ = server.accept() # wait for client

    header = 4
    message = 7

    client.recv(1024)
    client.sendall(header.to_bytes(4, byteorder='big', signed=True))
    client.sendall(message.to_bytes(4, byteorder='big', signed=True))

    client.close()
    server.close()


if __name__ == "__main__":
    main()
