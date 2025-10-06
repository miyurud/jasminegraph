import socket
import logging
import subprocess
import json
import requests
import re
from collections import Counter
import time

logging.basicConfig(level=logging.INFO, format="%(message)s")

HOST = "127.0.0.1"
PORT = 7777   # JasmineGraph master port
LINE_END = b"\r\n"

# ---------------------------
# Helpers
# ---------------------------
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

def query_jasminegraph(question: str) -> dict:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((HOST, PORT))
        logging.info(f"Connected to JasmineGraph master at {HOST}:{PORT}")

        sock.sendall(b"sbs" + LINE_END)
        _ = recv_until(sock, b"\n")  # discard prompt
        sock.sendall(b"36" + LINE_END)
        _ = recv_until(sock, b"\n")

        sock.sendall(question.encode("utf-8") + LINE_END)
        response = recv_until(sock, b"done\r")
        sock.sendall(b"exit" + LINE_END)

        try:
            return json.loads(response.strip())
        except Exception:
            return {"raw": response.strip()}

def format_graph_context(graph_data: dict) -> str:
    if "nodes" in graph_data and "paths" in graph_data:
        nodes = graph_data["nodes"]
        paths = graph_data["paths"]
        node_text = "\n".join([f"- {n}" for n in nodes])
        path_text = "\n".join([f"- {p}" for p in paths])
        return f"Nodes:\n{node_text}\n\nPaths:\n{path_text}"
    else:
        return json.dumps(graph_data, indent=2)

def ask_llama3(context: str, question: str) -> str:
    prompt = f"""You are given a graph substructure from a knowledge graph.
Use the nodes and paths to answer the question.
I am evaluating your answer based on F1 score. So just give me only the final answer. No explanation needed.

Graph Context:
{context}

Question: {question}
Answer:"""

    response = requests.post(
        "http://localhost:11434/api/generate",
        json={"model": "llama3", "prompt": prompt, "stream": False},
    )
    return response.json()["response"].strip()

# ---------------------------
# F1 Scoring
# ---------------------------
def normalize(text):
    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return text.strip().split()

def f1_score(prediction, ground_truth):
    pred_tokens = normalize(prediction)
    truth_tokens = normalize(ground_truth)
    common = Counter(pred_tokens) & Counter(truth_tokens)
    num_same = sum(common.values())
    if num_same == 0:
        return 0.0
    precision = num_same / len(pred_tokens)
    recall = num_same / len(truth_tokens)
    return 2 * precision * recall / (precision + recall)

# ---------------------------
# Validation Pipeline
# ---------------------------
def main():
    # Load QA dataset from JSON file
    with open("qa_pairs.json", "r", encoding="utf-8") as f:
        dataset = json.load(f)

    # Convert to simple list of dicts {q, a}
    qa_pairs = [{"q": item["question"], "a": item["answer"]} for item in dataset]

    scores = []
    for qa in qa_pairs:
        question, gold = qa["q"], qa["a"]
        start_time = time.time()

        # Step 2: GraphRAG query
        graph_data = query_jasminegraph(question)
        context = format_graph_context(graph_data)

        # Step 3: Llama3 answer
        pred = ask_llama3(context, question)
        end_time = time.time()
        logging.info(f"Time taken for Q&A: {end_time - start_time:.2f} seconds")

        # Step 4: F1 score
        score = f1_score(pred, gold)
        scores.append(score)

        print(f"Q: {question}")
        print(f"Gold: {gold}")
        print(f"Pred: {pred}")
        print(f"F1: {score:.2f}\n")

    print("Average F1:", sum(scores) / len(scores))

if __name__ == "__main__":
    main()
