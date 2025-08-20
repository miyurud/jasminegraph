#!/usr/bin/env python3
"""
semantic_beam_search_test.py
Test client for JasmineGraph's Semantic Beam Search endpoint.

Copyright 2025 JasmineGraph Team
Licensed under the Apache License, Version 2.0
"""

import socket
import logging

# --- Logging setup with colors ---
logging.addLevelName(
    logging.INFO, f"\033[1;32m{logging.getLevelName(logging.INFO)}\033[1;0m"
)
logging.addLevelName(
    logging.WARNING, f"\033[1;33m{logging.getLevelName(logging.WARNING)}\033[1;0m"
)
logging.addLevelName(
    logging.ERROR, f"\033[1;31m{logging.getLevelName(logging.ERROR)}\033[1;0m"
)
logging.addLevelName(
    logging.CRITICAL, f"\033[1;41m{logging.getLevelName(logging.CRITICAL)}\033[1;0m"
)

logging.getLogger().setLevel(logging.INFO)

# --- Protocol constants ---
HOST = "127.0.0.1"
PORT = 7780

SEMANTIC_BEAM_SEARCH = b"initiate-semantic-beam-search"
LINE_END = b"\r\n"

# In JasmineGraphInstanceProtocol, GRAPH_STREAM_END_OF_EDGE is CRLF
GRAPH_STREAM_END_OF_EDGE = b"\r\n"


def expect_response(conn: socket.socket, expected: bytes):
    """Wait for response and compare against expected"""
    buffer = bytearray()
    read = 0
    expected_len = len(expected)
    while read < expected_len:
        received = conn.recv(expected_len - read)
        if not received:
            logging.error("Connection closed unexpectedly")
            return False
        buffer.extend(received)
        read += len(received)
    data = bytes(buffer)
    print(">>", data.decode("utf-8"), end="")
    return data == expected


def send_with_length(sock: socket.socket, data: str):
    """Send a length-prefixed string to server (network byte order int + content)."""
    length = len(data)
    sock.sendall(length.to_bytes(4, byteorder="big"))
    sock.sendall(data.encode("utf-8"))


# Replace with the actual value of JasmineGraphInstanceProtocol::GRAPH_STREAM_C_length_ACK
GRAPH_STREAM_C_LENGTH_ACK = b"stream-c-length-ack"

def test_semantic_beam_search(sock: socket.socket):
    logging.info("Testing Semantic Beam Search")

    # Step 1: send command
    sock.sendall(SEMANTIC_BEAM_SEARCH + LINE_END)
    if not expect_response(sock, GRAPH_STREAM_C_LENGTH_ACK):
        logging.error("Server did not respond with OK after command")
        return

    # Step 2: send graphId
    graph_id = "2"
    send_with_length(sock, graph_id)
    if not expect_response(sock, GRAPH_STREAM_C_LENGTH_ACK):
        logging.error("Server did not ACK graphId length")
        return

    # Step 3: send partition id
    partition = "0"
    send_with_length(sock, partition)
    if not expect_response(sock, GRAPH_STREAM_C_LENGTH_ACK):
        logging.error("Server did not ACK partition length")
        return

    # Step 4: send query
    query = " when was Echosmith formed"
    send_with_length(sock, query)
    if not expect_response(sock, GRAPH_STREAM_END_OF_EDGE):
        logging.error("Did not receive end-of-stream marker from server")
        return

    logging.info("Semantic Beam Search test completed successfully")

def main():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((HOST, PORT))
        logging.info("Connected to JasmineGraph server at %s:%d", HOST, PORT)

        test_semantic_beam_search(sock)


if __name__ == "__main__":
    main()
