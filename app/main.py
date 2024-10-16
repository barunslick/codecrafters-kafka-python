import socket  # noqa: F401

VALID_API_VERSION = [1, 2, 3, 4]

## Helper functions
def parse_request_header_from_bytes(message: bytes):
    api_key = message[5:6]
    api_version = message[7:8]
    #The correlation_id is taken from bytes 8 to 12 (4 bytes)
    correlation_id = message[8:12]

    return {
        # Add more as we go on
        "api_key": api_key,
        "api_version": api_version,
        "correlation_id": correlation_id
    }

def send_to_client(client, message, byte_size):
    """
    Send an integer message to the connected client.

    Args:
        client (socket.socket): The socket object representing the client connection.
        message (int): The integer message to be sent.
        byte_size (int): The number of bytes to represent the integer.

    Description:
        - Converts the integer `message` to bytes and sends it to the client.
        - Uses big-endian byte order and treats the integer as signed.
    """
    client.sendall(message.to_bytes(byte_size, byteorder='big', signed=True))


def send_to_client_raw(client, message, pad_bytes_length = 0, pad_byte = b'\x00'):
    """
    Send a raw byte message to the connected client, with optional padding.

    Args:
        client (socket.socket): The socket object representing the client connection.
        message (bytes): The raw byte message to be sent.
        pad_bytes_length (int, optional): 
            The total length of the message after padding. Default is 0 (no padding).
        pad_byte (bytes, optional): 
            The byte used for padding. Default is b'\x00' (null byte).

    Description:
        - If `pad_bytes_length` is greater than the length of the original message,
          the function pads the message to the left (right-aligns it) using the specified `pad_byte`.
        - If `pad_bytes_length` is 0 or less than the message length, no padding is applied.
        - The final message is sent to the client using `sendall()`.

    Example:
        If `message` is b'\x12\x34' and `pad_bytes_length` is 4, 
        the sent message will be b'\x00\x00\x12\x34'.

    """
    if pad_bytes_length > 0:
        message = message.rjust(pad_bytes_length, pad_byte)
    client.sendall(message)


def main():
    server = socket.create_server(("localhost", 9092), reuse_port=True)

    client, _ = server.accept()

    dummy_header = 1
    message = client.recv(1024)

    request_header = parse_request_header_from_bytes(message)

    send_to_client(client, dummy_header, 4)
    send_to_client_raw(client, request_header["correlation_id"])

    if request_header["api_version"] not in VALID_API_VERSION :
        send_to_client(client, 35, 2)


    client.close()
    server.close()


if __name__ == "__main__":
    main()
