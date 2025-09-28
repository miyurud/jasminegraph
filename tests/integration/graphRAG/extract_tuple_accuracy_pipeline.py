#!/usr/bin/env python3
import json
import shutil
import socket
import logging
import os
import subprocess
import random
from tests.integration.graphRAG.fetch_pred import run_cypher_query, parse_results, OUTPUT_FILE

logging.basicConfig(level=logging.INFO, format="%(message)s")

# JasmineGraph master config
HOST = "127.0.0.1"
PORT = 7777
LINE_END = b"\r\n"

# Folder containing text files
TEXT_FOLDER = "pred_gemma3_12b"

# LLM runner addresses (comma-separated)
# LLM_RUNNERS = (
#     "http://192.168.1.7:6578,http://192.168.1.7:6578,"
#     "http://192.168.1.7:6578,http://192.168.1.7:6578"
# )


LLM_RUNNERS = (
    "http://192.168.1.7:11439,http://192.168.1.7:11439,"
    "http://192.168.1.7:11439,http://192.168.1.7:11439"
)
# LLM_RUNNERS = (
#     "https://sajeenthiranp-21--h100-gpu-node-4-llama3-70b-instruct-no-f1a8c3.modal.run,"
#     "https://sajeenthiranp-21--h100-gpu-node-4-llama3-70b-instruct-no-f1a8c3.modal.run"
# )
# https://sajeenthiranp-21--h100-gpu-node-4-llama3-70b-instruct-vl-9f6148.modal.run

# LLM model to use
# LLM_MODEL = "llama3"
LLM_MODEL  = "gemma3:12b"
# LLM_MODEL  = "meta-llama/Meta-Llama-3-70B-Instruct"
# LLM_MODEL  = "RedHatAI/Meta-Llama-3.1-8B-Instruct-FP8"

# HDFS target folder
HDFS_BASE = "/home/"

# Path to your bash script
UPLOAD_SCRIPT = "../utils/datasets/upload-hdfs-file.sh"

def recv_until(sock, stop=b"\n"):
    buffer = bytearray()
    while True:
        chunk = sock.recv(1)
        if not chunk:
            break
        buffer.extend(chunk)
        if buffer.endswith(stop):
            break
    return buffer.decode("utf-8")

def upload_to_hdfs(local_file):
    """Uploads a local file to HDFS using your bash script, renaming it to match the doc folder name"""
    # Get the parent folder name (doc folder)
    folder_name = os.path.basename(os.path.dirname(local_file))
    # Add .txt extension
    hdfs_filename = os.path.basename(local_file)
    hdfs_path = folder_name+"/"+hdfs_filename

    logging.info(f"Uploading {local_file} → HDFS:{hdfs_path}")
    # Use bash to run the upload script
    subprocess.run(["bash", UPLOAD_SCRIPT, local_file, folder_name], check=True)
    return hdfs_path
def send_file_to_master(hdfs_file_path):
    logging.info(f"Sending {hdfs_file_path} to JasmineGraph master")

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((HOST, PORT))
        logging.info(f"Connected to JasmineGraph master at {HOST}:{PORT}")

        # Start KG construction
        sock.sendall(b"constructkg" + LINE_END)

        # Step 1: Default HDFS
        msg = recv_until(sock, b"\n")
        logging.info("Master: " + msg.strip())
        sock.sendall(b"n" + LINE_END)  # Using custom HDFS config

        # Step 2: HDFS config path
        msg = recv_until(sock, b"\n")
        logging.info("Master: " + msg.strip())
        sock.sendall(b"/var/tmp/config/hdfs_config.txt" + LINE_END)

        # Step 3: HDFS dataset path
        msg = recv_until(sock, b"\n")
        logging.info("Master: " + msg.strip())
        sock.sendall(hdfs_file_path.encode("utf-8") + LINE_END)

        # Step 4: LLM runner addresses
        msg = recv_until(sock, b"\n")
        logging.info("Master: " + msg.strip())
        sock.sendall(LLM_RUNNERS.encode("utf-8") + LINE_END)

        # Step 5: LLM model
        msg = recv_until(sock, b"\n")
        logging.info("Master: " + msg.strip())
        sock.sendall(LLM_MODEL.encode("utf-8") + LINE_END)

        # Step 6: Wait for final confirmation
        final = recv_until(sock, b"\n")
        logging.info("Master: " + final.strip())

        if final.strip().lower() == "done":
            logging.info("✅ KG extraction completed successfully!")
        else:
            logging.error("❌ Unexpected response from master: " + final)

def get_last_graph_id():
    """Connect to JasmineGraph master, run 'lst', and return the last graph ID"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((HOST, PORT))
        logging.info(f"Connected to JasmineGraph master at {HOST}:{PORT}")

        # Send 'lst' command
        sock.sendall(b"lst" + b"\n")
        # sock.settimeout(1.0)
        print("test")

        # Receive all lines until no more data

        data = []
        while True:
            line = recv_until(sock, b"\r\n")
            if not line or "done" in line :   # EOF reached
                break
            print("LINE:", line.strip())  # <-- debug
            data.append(line.strip())


# Extract the last graph ID from the received lines
    graph_ids = []
    for line in data:
        # Expecting format: |1|American_Theocracy/text.txt|...
        parts = line.split("|")
        if len(parts) > 1 and parts[1].isdigit():
            graph_ids.append(int(parts[1]))

    if graph_ids:
        return max(graph_ids)
    return 0  # no graphs yet
def main():
    query = "MATCH (n)-[r]-(m) RETURN n,r,m"

    # Get last graph ID dynamically
    last_graph_id = get_last_graph_id()
    graph_id = last_graph_id + 1
    logging.info(f"Starting processing from graph ID {graph_id}")

    # Collect all .txt files from all subfolders
    all_txt_files = []
    for root, _, files in os.walk(TEXT_FOLDER):
        for file in files:
            if file.endswith(".txt"):
                all_txt_files.append(os.path.join(root, file))

    # Shuffle all files globally
    random.shuffle(all_txt_files)

    # Process each file
    for local_path in all_txt_files:
        folder_name = os.path.basename(os.path.dirname(local_path))

        try:
            hdfs_path = upload_to_hdfs(local_path)
            send_file_to_master(hdfs_path)
        except subprocess.CalledProcessError as e:
            logging.error(f"Failed to upload {local_path} to HDFS: {e}")
            continue

        raw = run_cypher_query(str(graph_id), query)
        triples = parse_results(raw)

        # Pretty-print triples to console
        print(json.dumps(triples, indent=2, ensure_ascii=False))

        # Prepare prediction output folder
        output_dir = os.path.join("pred", folder_name)
        os.makedirs(output_dir, exist_ok=True)

        # Save system predictions
        with open(os.path.join(output_dir, "pred.json"), "w", encoding="utf-8") as f:
            json.dump(triples, f, indent=2, ensure_ascii=False)

        # Copy gold files (entities.json, relations.json, text.txt)
        for gold_file in ["entities.json", "relations.json", "text.txt"]:
            src = os.path.join(TEXT_FOLDER, folder_name, gold_file)
            dst = os.path.join(output_dir, gold_file)
            if os.path.exists(src):
                shutil.copy(src, dst)

        graph_id += 1
if __name__ == "__main__":
    main()
