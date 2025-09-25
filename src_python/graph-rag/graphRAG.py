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
        sock.sendall(b"1" + LINE_END)
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
    # Step 1: QA Dataset (Ground Truth)
    qa_pairs = [
        {"q": "On what frequency does Radio City broadcast?", "a": "91.1 MHz"},
        {"q": "Which city was Radio City launched in 2004?", "a": "Mumbai"},
        # {"q": "Which languages of songs does Radio City play?", "a": "Hindi, English, and regional languages"},
        {"q": "Which city got Radio City in March 2006?", "a": "Hyderabad"},
        {"q": "Which city was added to Radio City’s network in October 2007?", "a": "Visakhapatnam"},
        {"q": "When was Radio City started?", "a": "3 July 2001"},
        {"q": "Where was Radio City first launched?", "a": "Bengaluru"},
        {"q": "Who is the CEO of Radio City?", "a": "Abraham Thomas"},
        {"q": "When was the Albanian National Team founded?", "a": "6 June 1930"},
        {"q": "When did Albania play its first international match?", "a": "1946"},
        {"q": "What is Echosmith best known for?", "a": "Cool Kids"},
        {"q": "Who founded Oberoi Hotels & Resorts?", "a": "Rai Bahadur Mohan Singh Oberoi"},
    ]

    scores = []
    for qa in qa_pairs:
        question, gold = qa["q"], qa["a"]
        start_time = time.time()

        # Step 2: GraphRAG query
        graph_data = query_jasminegraph(question)
        print(graph_data)
        # context = format_graph_context(graph_data)

        # Step 3: Llama3 answer
        pred = ask_llama3(graph_data, question)
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
