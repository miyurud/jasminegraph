import socket
import logging
import json
import sys
from time import sleep

# Configuration
HOST = "127.0.0.1"
PORT = 7777
GRAPH_ID = "8"
LINE_END = b"\r\n"
CYPHER = b"cypher"

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")


def extract_all_labels(graph_path):
    """Extract all unique labels found in nodes of the graph file."""
    labels = set()
    with open(graph_path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                entry = json.loads(line.strip())
                src_label = entry["source"]["properties"].get("label")
                dst_label = entry["destination"]["properties"].get("label")
                if src_label:
                    labels.add(src_label)
                if dst_label:
                    labels.add(dst_label)
            except Exception as e:
                logging.error("Error parsing line: %s", e)
    return labels


def extract_node_ids_by_label(graph_path, label):
    """Extract node IDs having a specific label from the graph file."""
    node_ids = set()
    with open(graph_path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                entry = json.loads(line.strip())
                src = entry["source"]
                dst = entry["destination"]

                if src["properties"].get("label") == label:
                    node_ids.add(str(src["id"]))
                if dst["properties"].get("label") == label:
                    node_ids.add(str(dst["id"]))
            except Exception as e:
                logging.error("Error parsing line: %s", e)
    return node_ids


def query_nodes_by_label(sock, graph_id, label):
    """Query all nodes with the label and return the set of node IDs."""
    try:
        sock.sendall(CYPHER + LINE_END)
        sleep(0.1)
        sock.recv(1024)  # Graph ID prompt
        sock.sendall(graph_id.encode() + LINE_END)
        sock.recv(1024)  # Input query prompt
        sock.sendall(f"MATCH(n:{label}) RETURN n".encode() + LINE_END)

        response = b""
        while True:
            chunk = sock.recv(4096)
            if not chunk:
                break
            response += chunk
            if b"done" in response.lower():
                break

        decoded = response.decode(errors="ignore")
        returned_ids = set()

        for line in decoded.splitlines():
            try:
                # Clean trailing commas, etc.
                clean_line = line.strip().rstrip(",")
                line_json = json.loads(clean_line)
                if "n" in line_json:
                    node = line_json["n"]
                    node_id = str(node.get("id"))
                    # Check label correctness in returned node
                    labels = node.get("labels") or node.get("label")
                    if isinstance(labels, str):
                        labels = [labels]
                    if label not in labels:
                        logging.warning("❌ Node ID %s missing expected label '%s'", node_id, label)
                    returned_ids.add(node_id)
            except Exception:
                continue

        return returned_ids

    except Exception as e:
        logging.error("Query failed for label '%s': %s", label, e)
        return set()


def validate_label(graph_path, graph_id, label):
    expected_ids = extract_node_ids_by_label(graph_path, label)
    logging.info("Label '%s': Expected node count: %d", label, len(expected_ids))

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((HOST, PORT))
        returned_ids = query_nodes_by_label(sock, graph_id, label)

    logging.info("Label '%s': Returned node count: %d", label, len(returned_ids))

    missing = expected_ids - returned_ids
    extra = returned_ids - expected_ids

    if not missing and not extra:
        logging.info("✅ Label '%s': All nodes correctly returned.", label)
    else:
        if missing:
            logging.warning("❌ Label '%s': Missing nodes: %s", label, sorted(missing))
        if extra:
            logging.warning("❌ Label '%s': Extra/unexpected nodes: %s", label, sorted(extra))
        logging.warning("❌ Label '%s': Validation failed. %d missing, %d extra", label, len(missing), len(extra))


if __name__ == "__main__":
    if len(sys.argv) > 1:
        graph_path = sys.argv[1]
        graph_id = sys.argv[2] if len(sys.argv) > 2 else GRAPH_ID
    else:
        graph_path = "../../integration/env_init/data/graph_with_properties_test2.txt"
        graph_id = GRAPH_ID

    all_labels = extract_all_labels(graph_path)
    logging.info("Found labels: %s", all_labels)

    for label in all_labels:
        validate_label(graph_path, graph_id, label)
