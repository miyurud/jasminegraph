"""Copyright 2025 JasmineGraph Team
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
#!/usr/bin/env python3
"""Knowledge Graph construction and Cypher query test runner."""

import json
import logging
import os
import random
import shutil
import socket
import subprocess
import time

import requests  # noqa: F401

logging.basicConfig(level=logging.INFO, format="%(message)s")

# JasmineGraph master config
RESULT = subprocess.check_output(["hostname", "-I"]).decode().strip()
SERVER_IP = RESULT.split()[0]
HOST = "127.0.0.1"
HDFS_PORT = "9000"
PORT = 7777
LINE_END = b"\r\n"

# Folder containing text files
TEXT_FOLDER = "gold"

# LLM runner configuration
LLM_RUNNERS = f"http://{SERVER_IP}:11441," * 2
RUNNER_URLS = [u.strip() for u in LLM_RUNNERS.split(",") if u.strip()]
REASONING_MODEL_URI = RUNNER_URLS[0] if RUNNER_URLS else None
LLM_MODEL = "gemma3:4b-it-qat"
LLM_INFERENCE_ENGINE = "ollama"

CHUNK_SIZE = "2048"

# HDFS target folder
HDFS_BASE = "/home/"

# Path to scripts
UPLOAD_SCRIPT = "../../utils/datasets/upload-hdfs-file.sh"
OLLAMA_SETUP_SCRIPT = "../utils/start-ollama.sh"


def recv_until(sock, stop=b"\n"):
    """Receive bytes from socket until a stop character."""
    buffer = bytearray()
    while True:
        chunk = sock.recv(1)
        if not chunk:
            break
        buffer.extend(chunk)
        if buffer.endswith(stop):
            break
    return buffer.decode("utf-8")


def upload_to_hdfs(local_file, upload_file_script):
    """Upload a local file to HDFS using the specified Bash script."""
    hdfs_filename = os.path.basename(local_file)
    hdfs_path = os.path.join(HDFS_BASE, hdfs_filename)

    logging.info("Uploading %s → HDFS:%s", local_file, hdfs_path)
    subprocess.run(["bash", upload_file_script, local_file, HDFS_BASE], check=True)
    return hdfs_path


def send_file_to_master(hdfs_file_path, host, port):
    """Send file path and configuration to JasmineGraph master."""
    logging.info("Sending %s to JasmineGraph master", hdfs_file_path)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((host, port))
        logging.info("Connected to JasmineGraph master at %s:%d", host, port)

        sock.sendall(b"constructkg" + LINE_END)
        msg = recv_until(sock, b"\n")
        logging.info("Master: %s", msg.strip())
        sock.sendall(b"n" + LINE_END)

        msg = recv_until(sock, b"\n")
        logging.info("Master: %s", msg.strip())
        sock.sendall(SERVER_IP.encode("utf-8") + LINE_END)

        msg = recv_until(sock, b"\n")
        logging.info("Master: %s", msg.strip())
        sock.sendall(HDFS_PORT.encode("utf-8") + LINE_END)

        msg = recv_until(sock, b"\n")
        logging.info("Master: %s", msg.strip())
        sock.sendall(hdfs_file_path.encode("utf-8") + LINE_END)

        msg = recv_until(sock, b"\n")
        logging.info("Master 101: %s", msg.strip())

        logging.info("LLM_RUNNERS: %s", LLM_RUNNERS)
        sock.sendall(LLM_RUNNERS.encode("utf-8") + LINE_END)

        msg = recv_until(sock, b"\n")
        logging.info("Master: %s", msg.strip())
        sock.sendall(LLM_INFERENCE_ENGINE.encode("utf-8") + LINE_END)

        msg = recv_until(sock, b"\n")
        logging.info("Master: %s", msg.strip())
        sock.sendall(LLM_MODEL.encode("utf-8") + LINE_END)

        msg = recv_until(sock, b"\n")
        logging.info("Master: %s", msg.strip())
        sock.sendall(CHUNK_SIZE.encode("utf-8") + LINE_END)

        final = recv_until(sock, b"\n").strip()

        if final == "There exists a graph with the file path, would you like to resume?":
            sock.sendall(b"n" + LINE_END)
            final = recv_until(sock, b"\n").strip()
            logging.info("Master: %s", final)
            sock.sendall(b"exit" + LINE_END)
            logging.info("KG extraction started successfully!")
        else:
            logging.info("Master: %s", final)
            sock.sendall(b"exit" + LINE_END)

        return final.split(":")[1] if ":" in final else final


def run_cypher_query(graph_id, query, host, port):
    """Run a Cypher query and return result rows."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((host, port))
        logging.info("Connected to JasmineGraph at %s:%d", host, port)

        sock.sendall(b"cypher" + LINE_END)
        recv_until(sock, b"\n")

        sock.sendall(graph_id.encode("utf-8") + LINE_END)
        recv_until(sock, b"\n")

        sock.sendall(query.encode("utf-8") + LINE_END)

        rows = []
        while True:
            line = recv_until(sock, b"\n").strip()
            if not line or "done" in line:
                break
            rows.append(line)

        sock.sendall(b"exit" + LINE_END)
        return rows


def parse_results(raw_rows):
    """Parse JSON rows from JasmineGraph into triples."""
    triples = []
    for row in raw_rows:
        try:
            data = json.loads(row)
            head = data["n"].get("name", data["n"].get("id"))
            tail = data["m"].get("name", data["m"].get("id"))
            rel = data["r"].get("type", "related_to")
            triples.append({"head_entity": head, "tail_entity": tail, "relation": rel})
        except (json.JSONDecodeError, KeyError) as err:
            logging.warning("Could not parse row: %s (%s)", row, err)
    return triples


def test_kg(text_folder, upload_file_script, host, port):
    """Upload files, construct KGs, query them, and store triples."""
    query = "MATCH (n)-[r]-(m) RETURN n,r,m"
    all_txt_files = []

    for root, _, files in os.walk(text_folder):
        for file in files:
            if file.endswith(".txt"):
                all_txt_files.append(os.path.join(root, file))

    random.shuffle(all_txt_files)

    for local_path in all_txt_files:
        folder_name = os.path.basename(os.path.dirname(local_path))
        try:
            hdfs_path = upload_to_hdfs(local_path, upload_file_script)
            graph_id = send_file_to_master(hdfs_path, host, port)
        except subprocess.CalledProcessError as err:
            logging.error("Failed to upload %s to HDFS: %s", local_path, err)
            continue

        # Wait for KG construction
        time.sleep(240)
        raw = run_cypher_query(str(graph_id), query, host, port)
        triples = parse_results(raw)
        print(json.dumps(triples, indent=2, ensure_ascii=False))

        output_dir = os.path.join("pred", folder_name)
        os.makedirs(output_dir, exist_ok=True)

        with open(os.path.join(output_dir, "pred.json"), "w", encoding="utf-8") as f:
            json.dump(triples, f, indent=2, ensure_ascii=False)

if __name__ == "__main__":
    test_kg(TEXT_FOLDER, UPLOAD_SCRIPT, HOST, PORT)
