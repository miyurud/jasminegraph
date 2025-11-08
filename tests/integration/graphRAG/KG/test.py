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
# from rapidfuzz import fuzz

# from tests.integration.graphRAG.fetch_pred import run_cypher_query, parse_results

# from tests.integration.graphRAG.fetch_pred import run_cypher_query, parse_results, OUTPUT_FILE



# Run hostname -I
result = subprocess.check_output(["hostname", "-I"]).decode().strip()

# Split by spaces and take the first IP
first_ip = result.split()[0]

print("First IP:", first_ip)
from concurrent.futures import ProcessPoolExecutor, as_completed

logging.basicConfig(level=logging.INFO, format="%(message)s")
SERVER_IP = first_ip
# JasmineGraph master config
HOST = '127.0.0.1'
HDFS_PORT ='9000'

PORT = 7777
LINE_END = b"\r\n"

# Folder containing text files
TEXT_FOLDER = "gold"

# LLM runner addresses (comma-separated)
LLM_RUNNERS = (
        f"http://{SERVER_IP}:11450," * 2
)
RUNNER_URLS = [u.strip() for u in LLM_RUNNERS.split(",") if u.strip()]
REASONING_MODEL_URI = RUNNER_URLS[0] if RUNNER_URLS else None
REASONING_MODEL_URI = f"http://{SERVER_IP}:11450"
# LLM model to use
# LLM_MODEL = "google/gemma-3-4b-it"
LLM_MODEL = " gemma3:12b"

# LLM_INFERENCE_ENGINE="vllm"
LLM_INFERENCE_ENGINE="ollama"

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
    hdfs_path = folder_name + "/" + hdfs_filename

    logging.info(f"Uploading {local_file} → HDFS:{hdfs_path}")
    subprocess.run(["bash", upload_file_script, local_file, folder_name], check=True)
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

        # msg = recv_until(sock, b"\n")
        # logging.info("Master: " + msg.strip())
        # sock.sendall(b"/var/tmp/config/hdfs_config.txt" + LINE_END)
        msg = recv_until(sock, b"\n")
        logging.info("Master: " + msg.strip())
        sock.sendall(SERVER_IP.encode("utf-8") + LINE_END)

        msg = recv_until(sock, b"\n")
        logging.info("Master: " + msg.strip())
        sock.sendall(HDFS_PORT.encode("utf-8")+ LINE_END)
        msg = recv_until(sock, b"\n")
        logging.info("Master: " + msg.strip())
        sock.sendall(hdfs_file_path.encode("utf-8") + LINE_END)
        #
        msg = recv_until(sock, b"\n")
        logging.info("Master 101: " + msg.strip())

        if msg.strip() == "There exists a graph with the file path, would you like to resume?":
            sock.sendall(b"n" + LINE_END)
            msg = recv_until(sock, b"\n")
            logging.info("Master 106: " + msg.strip())


            sock.sendall(LLM_RUNNERS.encode("utf-8") + LINE_END)


            msg5 = recv_until(sock, b"\n")
            logging.info("Master1133 : " + msg5.strip())
            sock.sendall(LLM_INFERENCE_ENGINE.encode("utf-8") + LINE_END)

            msg = recv_until(sock, b"\n")
            logging.info("Master: " + msg.strip())
            sock.sendall(LLM_MODEL.encode("utf-8") + LINE_END)

            msg = recv_until(sock, b"\n")
            logging.info("Master: " + msg.strip())
            sock.sendall(CHUNK_SIZE.encode("utf-8") + LINE_END)

            final = recv_until(sock, b"\n")
            logging.info("Master: " + final.strip())
            if final.strip().lower() == "done":
                sock.sendall(b"exit" + LINE_END)
                logging.info("✅ KG extraction completed successfully!")
            else:
                logging.error("❌ Unexpected response from master: " + final)


        else:
            print("LLM_RUNNERS 133 "+ LLM_RUNNERS)
            # msg = recv_until(sock, b"\n")
            logging.info("Master 135: " + msg.strip())
            sock.sendall(LLM_RUNNERS.encode("utf-8") + LINE_END)

            msg = recv_until(sock, b"\n")
            logging.info("Master: " + msg.strip())
            sock.sendall(LLM_INFERENCE_ENGINE.encode("utf-8")+ LINE_END)

            msg = recv_until(sock, b"\n")
            logging.info("Master: " + msg.strip())
            sock.sendall(LLM_MODEL.encode("utf-8") + LINE_END)

            msg = recv_until(sock, b"\n")
            logging.info("Master: " + msg.strip())
            sock.sendall(CHUNK_SIZE.encode("utf-8") + LINE_END)
            final = recv_until(sock, b"\n")
            logging.info("Master: " + final.strip())
            if final.strip()== "There exists a graph with the file path, would you like to resume?":
                sock.sendall(b"n" + LINE_END)
                final = recv_until(sock, b"\n")
                logging.info("Master: " + final.strip())

                sock.sendall(b"exit" + LINE_END)
                logging.info("✅ KG extraction completed successfully!")
            else:
                logging.info("Master: " + final.strip())
                sock.sendall(b"exit" + LINE_END)

            return final.strip().split(":")[1]







def get_last_graph_id():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((HOST, 7776))
        sock.sendall(b"lst\n")

        data = recv_until(sock, b"]")

        # while True:
        #     chunk = sock.recv(4096)
        #     if not chunk:
        #         break
        #     data += chunk

        sock.sendall(b"exit\n")

    # Try to decode the JSON response
    try:
        graphs = json.loads(data.strip())
    except json.JSONDecodeError as e:
        print("JSON decode error:", e)
        print("Raw data received:", data)
        return 0

    # Extract and return the highest graph ID
    graph_ids = [g.get("idgraph", 0) for g in graphs if isinstance(g, dict)]
    return max(graph_ids) if graph_ids else 0
def call_reasoning_model(prompt):
    """
    Call reasoning model (Ollama or vLLM) using the first runner URL.
    Uses LLM_INFERENCE_ENGINE ('ollama' | 'vllm') and LLM_MODEL.
    """
    if not REASONING_MODEL_URI:
        logging.error("❌ No valid runner URL found in LLM_RUNNERS")
        return ""

    try:
        if LLM_INFERENCE_ENGINE.lower() == "ollama":
            url = f"{REASONING_MODEL_URI}/api/generate"
            payload = {
                "model": LLM_MODEL,
                "prompt": prompt,
                "stream": False
            }
            resp = requests.post(url, json=payload, timeout=120)
            resp.raise_for_status()
            data = resp.json()
            return data.get("response", "").strip()

        elif LLM_INFERENCE_ENGINE.lower() == "vllm":
            url = f"{REASONING_MODEL_URI}/v1/chat/completions"
            payload = {
                "model": LLM_MODEL,
                "messages": [
                    {"role": "system", "content": "You are a reasoning assistant."},
                    {"role": "user", "content": prompt}
                ],
                "temperature": 0.2,
                "max_tokens": 512
            }
            resp = requests.post(url, json=payload, timeout=120)
            resp.raise_for_status()
            data = resp.json()
            return data["choices"][0]["message"]["content"].strip()

        else:
            raise ValueError(f"Unsupported engine: {LLM_INFERENCE_ENGINE}")

    except Exception as e:
        logging.error(f"❌ Reasoning model call failed: {e}")
        return ""
def compress_triples(triples):
    """
    Compress triples to short string representation:
    [{"head_entity":"A","relation":"is","tail_entity":"B"}] -> ["A|is|B"]
    """
    compressed = [f"{t['head_entity']}|{t['relation']}|{t['tail_entity']}" for t in triples]
    return compressed
def call_llm_ollama(prompt):
    """Call Ollama API and return model response"""

    endpoint= "/api/generate"
    url = REASONING_MODEL_URI + endpoint
    payload = {
        "model": "deepseek-r1",
        "prompt": prompt,
        "stream": False
    }
    try:
        resp = requests.post(url, json=payload, timeout=120)
        resp.raise_for_status()
        data = resp.json()
        print("LLM response:"+str(data))
        return data.get("response", "").strip()
    except Exception as e:
        logging.error(f"❌ LLM call failed: {e}")
        return ""
def extract_final_answer(text: str) -> str:
    # Remove <think>...</think> reasoning if present
    if "<think>" in text and "</think>" in text:
        text = text.split("</think>")[-1].strip()
    # Take only the first line / short phrase
    return text


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
            print("why")
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
            rel  = data["r"].get("type", "related_to")
            triples.append({
                "head_entity": head,
                "tail_entity": tail,
                "relation": rel
            })
        except Exception as e:
            logging.warning(f"Could not parse row: {row} ({e})")
    return triples

def wait_until_graph_ready(HOST, PORT):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((HOST, PORT))

        while True:
            sock.sendall(b"lst")

            buffer = b""
            bracket_count = 0
            started = False

            while True:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                buffer += chunk

                # decode incrementally to count brackets
                text = chunk.decode(errors="ignore")
                for ch in text:
                    if ch == '[':
                        started = True
                        bracket_count += 1
                    elif ch == ']':
                        bracket_count -= 1

                # stop only when we started and brackets are balanced
                if started and bracket_count == 0:
                    break

            raw = buffer.decode(errors="ignore").strip()
            if not raw:
                print("[WARN] Empty response")
                time.sleep(5)
                continue

            start = raw.find("[")
            end = raw.rfind("]")
            if start == -1 or end == -1:
                print("[WARN] No JSON brackets found in response")
                time.sleep(5)
                continue

            json_text = raw[start:end + 1]

            try:
                graphs = json.loads(json_text)
            except json.JSONDecodeError as e:
                print(f"[WARN] JSON parse failed: {e}")
                print("Raw JSON candidate:", repr(json_text))
                time.sleep(5)
                continue

            if not graphs:
                print("[WARN] Empty graph list")
                time.sleep(5)
                continue

            last_graph = graphs[-1]
            status = last_graph.get("status", "").lower()
            graph_id = last_graph.get("idgraph")
            print(f"Graph {graph_id} status: {status}")

            if status == "nop":
                time.sleep(10)
            else:
                break

        try:
            sock.sendall(b"exit")
        except Exception:
            pass

    print("✅ Graph is ready:", last_graph)
    return last_graph
def test_KG(llm_inference_engine_startup_script, text_folder , upload_file_script):

    query = "MATCH (n)-[r]-(m) RETURN n,r,m"

    # start the llm inference engine
    # subprocess.run(["bash", llm_inference_engine_startup_script], check=True)


    last_graph_id = get_last_graph_id()
    graph_id = last_graph_id + 1
    logging.info(f"Starting processing from graph ID {graph_id}")

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
            hdfs_path = upload_to_hdfs(local_path , upload_file_script)
            graph_id = send_file_to_master(hdfs_path)
        except subprocess.CalledProcessError as e:
            logging.error(f"Failed to upload {local_path} to HDFS: {e}")
            continue

        # with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        #     sock.connect((HOST, PORT))
        #
        #     while True:
        #         sock.sendall(b"lst" + b"\n")
        #
        #         data = []
        #         while True:
        #             line = recv_until(sock, b"\r\n")
        #             if not line or "done" in line:
        #                 break
        #             data.append(line.strip())
        #
        #         graph_ids = []
        #         print(data)
        #         print( data[-1].split("|"))
        #         if "nop" == data[-1].split("|")[4]:
        #             time.sleep(10)
        #         else:break
        wait_until_graph_ready(HOST, 7776)
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

        # QA prediction
        qa_file = os.path.join(text_folder, folder_name, "qa_pairs.json")
        if os.path.exists(qa_file):
            with open(qa_file, "r", encoding="utf-8") as f:
                qa_data = json.load(f)



            question = qa_data["question"]
            answer = qa_data["answer"]
            compressed_triples_ = compress_triples(triples)
            prompt = f"""You are a QA assistant.
Given the extracted triples:
{json.dumps(compressed_triples_, indent=2, ensure_ascii=False)}

Question: {question}
 Answer in a short phrase. Do not include reasoning, explanations, or extra text."""



            predicted_answer = extract_final_answer(call_reasoning_model(prompt))

            pred_out = {
                "id": qa_data["id"],
                "question": question,
                "gold_answer": answer,
                "predicted_answer": predicted_answer
            }
            with open(os.path.join(output_dir, "pred_answer.json"), "w", encoding="utf-8") as f:
                json.dump(pred_out, f, indent=2, ensure_ascii=False)

        graph_id += 1

# def evaluate_predictions_fuzzy(pred_base="pred", threshold=90):
#     """
#     Evaluate QA predictions with fuzzy matching.
#     threshold: percentage similarity to count as correct
#     """
#     total = 0
#     correct = 0
#     mismatches = []
#
#     for root, _, files in os.walk(pred_base):
#         for file in files:
#             if file == "pred_answer.json":
#                 pred_file = os.path.join(root, file)
#                 with open(pred_file, "r", encoding="utf-8") as f:
#                     pred_data = json.load(f)
#
#                 gold = pred_data["gold_answer"].strip().lower()
#                 pred = pred_data["predicted_answer"].strip().lower()
#
#                 total += 1
#                 score = fuzz.ratio(pred, gold)
#                 if score >= threshold:
#                     correct += 1
#                 else:
#                     mismatches.append({
#                         "id": pred_data["id"],
#                         "question": pred_data["question"],
#                         "gold_answer": pred_data["gold_answer"],
#                         "predicted_answer": pred_data["predicted_answer"],
#                         "similarity": score
#                     })
#
#     accuracy = correct / total if total > 0 else 0
#     print("\n=== Fuzzy Evaluation ===")
#     print(f"Total QA pairs: {total}")
#     print(f"Correct predictions (>= {threshold}% similarity): {correct}")
#     print(f"Accuracy: {accuracy:.2%}")
#
#     if mismatches:
#         print("\nMismatched predictions (below threshold):")
#         for m in mismatches:
#             print(f"ID: {m['id']}, Q: {m['question']}")
#             print(f"  Gold: {m['gold_answer']}, Pred: {m['predicted_answer']}, Similarity: {m['similarity']:.1f}%\n")


# def process_file(local_path, graph_id):
#     folder_name = os.path.basename(os.path.dirname(local_path))
#     try:
#         hdfs_path = upload_to_hdfs(local_path)
#         send_file_to_master(hdfs_path)
#     except subprocess.CalledProcessError as e:
#         logging.error(f"Failed to upload {local_path} to HDFS: {e}")
#         return None
#
#     query = "MATCH (n)-[r]-(m) RETURN n,r,m"
#     raw = run_cypher_query(str(graph_id), query)
#     triples = parse_results(raw)
#
#     output_dir = os.path.join("pred", folder_name)
#     os.makedirs(output_dir, exist_ok=True)
#
#     with open(os.path.join(output_dir, "pred.json"), "w", encoding="utf-8") as f:
#         json.dump(triples, f, indent=2, ensure_ascii=False)
#
#     # Copy gold files
#     for gold_file in ["entities.json", "relations.json", "text.txt"]:
#         src = os.path.join(TEXT_FOLDER, folder_name, gold_file)
#         dst = os.path.join(output_dir, gold_file)
#         if os.path.exists(src):
#             shutil.copy(src, dst)
#
#     # QA prediction
#     qa_file = os.path.join(TEXT_FOLDER, folder_name, "qa_pairs.json")
#     if os.path.exists(qa_file):
#         with open(qa_file, "r", encoding="utf-8") as f:
#             qa_data = json.load(f)
#
#         question = qa_data["question"]
#         answer = qa_data["answer"]
#
#         prompt = f"""You are a QA assistant.
# Given the extracted triples:
# {json.dumps(triples, indent=2, ensure_ascii=False)}
#
# Question: {question}
# Answer in a short phrase."""
#
#         predicted_answer = call_llm_ollama(prompt)
#
#         pred_out = {
#             "id": qa_data["id"],
#             "question": question,
#             "gold_answer": answer,
#             "predicted_answer": predicted_answer
#         }
#         with open(os.path.join(output_dir, "pred_answer.json"), "w", encoding="utf-8") as f:
#             json.dump(pred_out, f, indent=2, ensure_ascii=False)
#
#     return graph_id
# def main():
#     last_graph_id = get_last_graph_id()
#     graph_id = last_graph_id + 1
#     logging.info(f"Starting processing from graph ID {graph_id}")
#
#     all_txt_files = []
#     for root, _, files in os.walk(TEXT_FOLDER):
#         for file in files:
#             if file.endswith(".txt"):
#                 all_txt_files.append(os.path.join(root, file))
#
#     random.shuffle(all_txt_files)
#     print(all_txt_files)
#
#     # Run max 5 files in parallel
#     with ProcessPoolExecutor(max_workers=5) as executor:
#         future_to_file = {
#             executor.submit(process_file, path, gid): (path, gid)
#             for gid, path in enumerate(all_txt_files, start=graph_id)
#         }
#
#         for future in as_completed(future_to_file):
#             path, gid = future_to_file[future]
#             try:
#                 res = future.result()
#                 logging.info(f"Finished {path} with graph ID {res}")
#             except Exception as e:
#                 logging.error(f"Error processing {path}: {e}")

if __name__ == "__main__":
    test_KG(OLLAMA_SETUP_SCRIPT , TEXT_FOLDER ,UPLOAD_SCRIPT)
    # evaluate_predictions_fuzzy(pred_base="pred", threshold=90)
