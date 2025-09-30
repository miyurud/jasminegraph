#!/usr/bin/env python3
import json
import shutil
import socket
import logging
import os
import subprocess
import random
import requests
from rapidfuzz import fuzz
from tests.integration.graphRAG.fetch_pred import run_cypher_query, parse_results, OUTPUT_FILE

logging.basicConfig(level=logging.INFO, format="%(message)s")

# JasmineGraph master config
HOST = "127.0.0.1"
PORT = 7777
LINE_END = b"\r\n"

# Folder containing text files
TEXT_FOLDER = "hotpot_qa_records"

# LLM runner addresses (comma-separated)
LLM_RUNNERS = (
    "http://10.10.21.26:11439,http://10.10.21.26:11439,"
    "http://10.10.21.26:11439,http://10.10.21.26:11439"
)

# LLM model to use
LLM_MODEL = "gemma3:12b"

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
    """Uploads a local file to HDFS using your bash script"""
    folder_name = os.path.basename(os.path.dirname(local_file))
    hdfs_filename = os.path.basename(local_file)
    hdfs_path = folder_name + "/" + hdfs_filename

    logging.info(f"Uploading {local_file} → HDFS:{hdfs_path}")
    subprocess.run(["bash", UPLOAD_SCRIPT, local_file, folder_name], check=True)
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
        sock.sendall(b"/var/tmp/config/hdfs_config.txt" + LINE_END)

        msg = recv_until(sock, b"\n")
        logging.info("Master: " + msg.strip())
        sock.sendall(hdfs_file_path.encode("utf-8") + LINE_END)

        msg = recv_until(sock, b"\n")
        logging.info("Master: " + msg.strip())
        sock.sendall(LLM_RUNNERS.encode("utf-8") + LINE_END)

        msg = recv_until(sock, b"\n")
        logging.info("Master: " + msg.strip())
        sock.sendall(LLM_MODEL.encode("utf-8") + LINE_END)

        final = recv_until(sock, b"\n")
        logging.info("Master: " + final.strip())
        if final.strip().lower() == "done":
            logging.info("✅ KG extraction completed successfully!")
        else:
            logging.error("❌ Unexpected response from master: " + final)

def get_last_graph_id():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((HOST, PORT))
        sock.sendall(b"lst" + b"\n")

        data = []
        while True:
            line = recv_until(sock, b"\r\n")
            if not line or "done" in line:
                break
            data.append(line.strip())

    graph_ids = []
    for line in data:
        parts = line.split("|")
        if len(parts) > 1 and parts[1].isdigit():
            graph_ids.append(int(parts[1]))

    if graph_ids:
        return max(graph_ids)
    return 0

def call_llm_ollama(prompt):
    """Call Ollama API and return model response"""
    url = LLM_RUNNERS.split(",")[0] + "/api/generate"
    payload = {
        "model": LLM_MODEL,
        "prompt": prompt,
        "stream": False
    }
    try:
        resp = requests.post(url, json=payload, timeout=120)
        resp.raise_for_status()
        data = resp.json()
        return data.get("response", "").strip()
    except Exception as e:
        logging.error(f"❌ LLM call failed: {e}")
        return ""

def main():
    query = "MATCH (n)-[r]-(m) RETURN n,r,m"
    last_graph_id = get_last_graph_id()
    graph_id = last_graph_id + 1
    logging.info(f"Starting processing from graph ID {graph_id}")

    all_txt_files = []
    for root, _, files in os.walk(TEXT_FOLDER):
        for file in files:
            if file.endswith(".txt"):
                all_txt_files.append(os.path.join(root, file))

    random.shuffle(all_txt_files)
    print(all_txt_files)

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

        print(json.dumps(triples, indent=2, ensure_ascii=False))

        output_dir = os.path.join("pred", folder_name)
        os.makedirs(output_dir, exist_ok=True)

        with open(os.path.join(output_dir, "pred.json"), "w", encoding="utf-8") as f:
            json.dump(triples, f, indent=2, ensure_ascii=False)

        # Copy gold files
        for gold_file in ["entities.json", "relations.json", "text.txt"]:
            src = os.path.join(TEXT_FOLDER, folder_name, gold_file)
            dst = os.path.join(output_dir, gold_file)
            if os.path.exists(src):
                shutil.copy(src, dst)

        # QA prediction
        qa_file = os.path.join(TEXT_FOLDER, folder_name, "qa_pairs.json")
        if os.path.exists(qa_file):
            with open(qa_file, "r", encoding="utf-8") as f:
                qa_data = json.load(f)

            question = qa_data["question"]
            answer = qa_data["answer"]

            prompt = f"""You are a QA assistant. 
Given the extracted triples:
{json.dumps(triples, indent=2, ensure_ascii=False)}

Question: {question}
Answer in a short phrase."""

            predicted_answer = call_llm_ollama(prompt)

            pred_out = {
                "id": qa_data["id"],
                "question": question,
                "gold_answer": answer,
                "predicted_answer": predicted_answer
            }
            with open(os.path.join(output_dir, "pred_answer.json"), "w", encoding="utf-8") as f:
                json.dump(pred_out, f, indent=2, ensure_ascii=False)

        graph_id += 1

def evaluate_predictions_fuzzy(pred_base="pred", threshold=90):
    """
    Evaluate QA predictions with fuzzy matching.
    threshold: percentage similarity to count as correct
    """
    total = 0
    correct = 0
    mismatches = []

    for root, _, files in os.walk(pred_base):
        for file in files:
            if file == "pred_answer.json":
                pred_file = os.path.join(root, file)
                with open(pred_file, "r", encoding="utf-8") as f:
                    pred_data = json.load(f)

                gold = pred_data["gold_answer"].strip().lower()
                pred = pred_data["predicted_answer"].strip().lower()

                total += 1
                score = fuzz.ratio(pred, gold)
                if score >= threshold:
                    correct += 1
                else:
                    mismatches.append({
                        "id": pred_data["id"],
                        "question": pred_data["question"],
                        "gold_answer": pred_data["gold_answer"],
                        "predicted_answer": pred_data["predicted_answer"],
                        "similarity": score
                    })

    accuracy = correct / total if total > 0 else 0
    print("\n=== Fuzzy Evaluation ===")
    print(f"Total QA pairs: {total}")
    print(f"Correct predictions (>= {threshold}% similarity): {correct}")
    print(f"Accuracy: {accuracy:.2%}")

    if mismatches:
        print("\nMismatched predictions (below threshold):")
        for m in mismatches:
            print(f"ID: {m['id']}, Q: {m['question']}")
            print(f"  Gold: {m['gold_answer']}, Pred: {m['predicted_answer']}, Similarity: {m['similarity']:.1f}%\n")

if __name__ == "__main__":
    main()
    evaluate_predictions_fuzzy(pred_base="pred", threshold=90)
