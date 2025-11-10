#!/usr/bin/env python3
import json
import shutil
import socket
import logging
import os
import subprocess
import random
import time


import requests

result = subprocess.check_output(["hostname", "-I"]).decode().strip()

# Split by spaces and take the first IP
first_ip = result.split()[0]


logging.basicConfig(level=logging.INFO, format="%(message)s")
SERVER_IP = first_ip
# JasmineGraph master config
HOST = "127.0.0.1"
HDFS_PORT = "9000"

PORT = 7777
LINE_END = b"\r\n"

# Folder containing text files
TEXT_FOLDER = "gold"

# LLM runner addresses (comma-separated)
LLM_RUNNERS = f"http://{SERVER_IP}:11441," * 2
RUNNER_URLS = [u.strip() for u in LLM_RUNNERS.split(",") if u.strip()]
REASONING_MODEL_URI = RUNNER_URLS[0] if RUNNER_URLS else None
REASONING_MODEL_URI = f"http://{SERVER_IP}:11441"
LLM_MODEL = "gemma3:4b-it-qat"
LLM_INFERENCE_ENGINE = "ollama"

CHUNK_SIZE = "2048"
# HDFS target folder
HDFS_BASE = "/home/"

# Path to your bash script
UPLOAD_SCRIPT = "../../utils/datasets/upload-hdfs-file.sh"
OLLAMA_SETUP_SCRIPT = "../utils/start-ollama.sh"


def recv_until(sock, stop=b"\n"):
    buffer = bytearray()
    while True:
        chunk = sock.recv(1)
        # print ( chunk.decode("utf-8"))
        if not chunk:
            break
        buffer.extend(chunk)
        if buffer.endswith(stop):
            break
    return buffer.decode("utf-8")


def upload_to_hdfs(local_file, upload_file_script):
    """Uploads a local file to HDFS using your bash script"""
    folder_name = os.path.basename(os.path.dirname(local_file))
    hdfs_filename = os.path.basename(local_file)
    hdfs_path = "/home/" + hdfs_filename

    logging.info(f"Uploading {local_file} → HDFS:{hdfs_path}")
    subprocess.run(["bash", upload_file_script, local_file, "/home/"], check=True)
    return hdfs_path


def send_file_to_master(hdfs_file_path):
    logging.info(f"Sending {hdfs_file_path} to JasmineGraph master")
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((HOST, PORT))
        logging.info(f"Connected to JasmineGraph master at {HOST}:{PORT}")

        sock.sendall(b"constructkg" + LINE_END)
        msg = recv_until(sock, b"\n")
        logging.info("Master: " + msg.strip())
        sock.sendall(b"n" + LINE_END)


        msg = recv_until(sock, b"\n")
        logging.info("Master: " + msg.strip())
        sock.sendall(SERVER_IP.encode("utf-8") + LINE_END)

        msg = recv_until(sock, b"\n")
        logging.info("Master: " + msg.strip())
        sock.sendall(HDFS_PORT.encode("utf-8") + LINE_END)
        msg = recv_until(sock, b"\n")
        logging.info("Master: " + msg.strip())
        sock.sendall(hdfs_file_path.encode("utf-8") + LINE_END)
        #
        msg = recv_until(sock, b"\n")
        logging.info("Master 101: " + msg.strip())


        print("LLM_RUNNERS  " + LLM_RUNNERS)
        logging.info("Master : " + msg.strip())
        sock.sendall(LLM_RUNNERS.encode("utf-8") + LINE_END)

        msg = recv_until(sock, b"\n")
        logging.info("Master: " + msg.strip())
        sock.sendall(LLM_INFERENCE_ENGINE.encode("utf-8") + LINE_END)

        msg = recv_until(sock, b"\n")
        logging.info("Master: " + msg.strip())
        sock.sendall(LLM_MODEL.encode("utf-8") + LINE_END)

        msg = recv_until(sock, b"\n")
        logging.info("Master: " + msg.strip())
        sock.sendall(CHUNK_SIZE.encode("utf-8") + LINE_END)
        final = recv_until(sock, b"\n")
        logging.info("Master: " + final.strip())
        if (
                final.strip()
                == "There exists a graph with the file path, would you like to resume?"
        ):
            sock.sendall(b"n" + LINE_END)
            final = recv_until(sock, b"\n")
            logging.info("Master: " + final.strip())

            sock.sendall(b"exit" + LINE_END)
            logging.info("KG extraction started successfully!")
        else:
            logging.info("Master: " + final.strip())
            sock.sendall(b"exit" + LINE_END)

        return final.strip().split(":")[1]


def run_cypher_query(graph_id: str, query: str):
    """Run a Cypher query and return raw rows"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((HOST, PORT))
        logging.info(f"Connected to JasmineGraph at {HOST}:{PORT}")

        # Step 1: Send 'cypher'
        sock.sendall(b"cypher" + LINE_END)
        recv_until(sock, b"\n")  # "Graph ID:"

        # Step 2: Send graph id
        sock.sendall(graph_id.encode("utf-8") + LINE_END)
        recv_until(sock, b"\n")  # "Input query :"

        # Step 3: Send query
        sock.sendall(query.encode("utf-8") + LINE_END)

        rows = []
        while True:

            line = recv_until(sock, b"\n").strip()
            print(line)
            if not line or "done" in line:
                break
            rows.append(line)

        # Close Cypher session
        sock.sendall(b"exit" + LINE_END)
        # recv_until(sock, b"\n")

        return rows


def parse_results(raw_rows):
    """Convert raw JSON rows from JasmineGraph to head-tail-relation triples"""
    triples = []
    for row in raw_rows:
        try:
            data = json.loads(row)
            head = data["n"].get("name", data["n"].get("id"))
            tail = data["m"].get("name", data["m"].get("id"))
            rel = data["r"].get("type", "related_to")
            triples.append({"head_entity": head, "tail_entity": tail, "relation": rel})
        except Exception as e:
            logging.warning(f"Could not parse row: {row} ({e})")
    return triples


def test_KG(llm_inference_engine_startup_script, text_folder, upload_file_script):

    query = "MATCH (n)-[r]-(m) RETURN n,r,m"

    # start the llm inference engine
    # subprocess.run(["bash", llm_inference_engine_startup_script], check=True)

    all_txt_files = []
    for root, _, files in os.walk(text_folder):
        print(files)
        for file in files:
            if file.endswith(".txt"):
                all_txt_files.append(os.path.join(root, file))

    random.shuffle(all_txt_files)
    print(all_txt_files)

    for local_path in all_txt_files:
        folder_name = os.path.basename(os.path.dirname(local_path))
        try:
            hdfs_path = upload_to_hdfs(local_path, upload_file_script)
            graph_id = send_file_to_master(hdfs_path)
        except subprocess.CalledProcessError as e:
            logging.error(f"Failed to upload {local_path} to HDFS: {e}")
            continue


        time.sleep(120)
        # wait_until_complete(HOST, 7776, graph_id)
        raw = run_cypher_query(str(graph_id), query)
        triples = parse_results(raw)

        print(json.dumps(triples, indent=2, ensure_ascii=False))

        output_dir = os.path.join("pred", folder_name)
        os.makedirs(output_dir, exist_ok=True)

        with open(os.path.join(output_dir, "pred.json"), "w", encoding="utf-8") as f:
            json.dump(triples, f, indent=2, ensure_ascii=False)

        # Copy gold files
        for gold_file in ["entities.json", "relations.json", "text.txt"]:
            src = os.path.join(text_folder, folder_name, gold_file)
            dst = os.path.join(output_dir, gold_file)
            if os.path.exists(src):
                shutil.copy(src, dst)


if __name__ == "__main__":
    test_KG(OLLAMA_SETUP_SCRIPT, TEXT_FOLDER, UPLOAD_SCRIPT)
