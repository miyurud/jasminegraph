#!/usr/bin/env python3
import socket
import logging

logging.basicConfig(level=logging.INFO, format="%(message)s")

HOST = "127.0.0.1"
PORT = 7777   # Master port
LINE_END = b"\r\n"

def recv_until(sock, stop=b"\n"):
    """Receive data until stop marker"""
    buffer = bytearray()
    while True:
        chunk = sock.recv(1)
        if not chunk:
            break
        buffer.extend(chunk)
        if buffer.endswith(stop):
            break
    return buffer.decode("utf-8")

def main():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((HOST, PORT))
        logging.info(f"Connected to JasmineGraph master at {HOST}:{PORT}")
        sock.sendall(b"constructkg" + LINE_END)

        # --- Step 1: expect "Do you want to use the default HDFS server(y/n)?"
        msg1 = recv_until(sock, b"\n")
        logging.info("Master: " + msg1.strip())

        # Send "n" (use custom HDFS config)
        sock.sendall(b"n" + LINE_END)

        # --- Step 2: expect "Send the file path to the HDFS configuration file"
        msg2 = recv_until(sock, b"\n")
        logging.info("Master: " + msg2.strip())

        # Send path to HDFS config file
        sock.sendall(b"/var/tmp/config/hdfs_config.txt" + LINE_END)

        # --- Step 3: expect "HDFS file path:"
        msg3 = recv_until(sock, b"\n")
        logging.info("Master: " + msg3.strip())

        # Send the HDFS dataset path
        sock.sendall(b"/home/hotpotqa_full_corpus.txt" + LINE_END)

        # --- Step 4: wait for final "done"
        final = recv_until(sock, b"\n")
        logging.info("Master: " + final.strip())

        if final.strip().lower() == "done":
            logging.info("✅ KG streaming completed successfully!")
        else:
            logging.error("❌ Unexpected response from master: " + final)

if __name__ == "__main__":
    main()
