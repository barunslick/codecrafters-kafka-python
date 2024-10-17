import select
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

def int_to_bytes(value, byte_size):
    return value.to_bytes(byte_size, byteorder='big', signed=True)

def bytes_to_int(value):
    return int.from_bytes(value, 'big', signed=True)

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


def create_api_versions_response():
    """
    Create a response message for the API versions supported by the server.
    Currently only sends for ApiKey 18
    """

    num_of_api_versions = int_to_bytes(2, 1)
    supported_api_versions = int_to_bytes(18, 2)
    min_supported_api_version_18 = int_to_bytes(0, 2)
    max_supported_api_version_18 = int_to_bytes(4, 2)

    return b''.join([num_of_api_versions, supported_api_versions, min_supported_api_version_18, max_supported_api_version_18])



def main():
    server = socket.create_server(("localhost", 9092), reuse_port=True)

    clients = [server]

    server.setblocking(False)

    while(True):

        readable, _, _ = select.select(clients, [], [], 0.1)

        for s in readable:
            if s is server:
                client, _ = server.accept()
                clients.append(client)
                continue

            try:
                req_message = s.recv(1024)

                if not req_message:
                    s.close()
                    clients.remove(s)
                    continue

                request_header = parse_request_header_from_bytes(req_message)

                message = request_header["correlation_id"].rjust(4, b'\x00')
                
                error_code = int_to_bytes(0, 2) if bytes_to_int(request_header["api_version"]) in VALID_API_VERSION else int_to_bytes(35, 2)
                
                message += error_code
                message += create_api_versions_response()
                message += int_to_bytes(0, 6) # TAG_BUFFER (2 Bytes) + throttle_time_ms (4 bytes)

                size_of_message = len(message)
                message = int_to_bytes(size_of_message, 4) + message

                send_to_client_raw(s, message)
            except socket.error:
                s.close()
                clients.remove(s)
                continue

    server.close()


if __name__ == "__main__":
    main()
