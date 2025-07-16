"""This script connects to a server to validate an uploaded graph by sending Cypher queries."""
import socket
import logging
import json
import random
import sys
from time import sleep

# Configuration
HOST = "127.0.0.1"
PORT = 7777
GRAPH_ID = "47"
LINE_END = b"\r\n"
CYPHER = b"cypher"

# Logging setup
logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
failed_tests = []


def send_query(sock, test_name, query, expected_keywords, graph_id):
    """Send a query to the server and validate the response against expected keywords."""
    try:
        # Start CYPHER session for this query
        sock.sendall(CYPHER + LINE_END)
        sleep(0.1)
        sock.recv(1024)  # Expect "Graph ID:"
        sleep(0.1)
        sock.sendall(graph_id.encode() + LINE_END)
        sock.recv(1024)  # Expect "Input query :"
        sleep(0.1)
        sock.sendall(query.encode() + LINE_END)

        response = b""
        while True:
            chunk = sock.recv(4096)
            if not chunk:
                break
            response += chunk
            if b"done" in response.lower():
                break

        logging.info(
            "\n[%s] Query: %s\nResponse:%s\n",
            test_name,
            query,
            response.strip(),
        )

        if all(keyword in response for keyword in expected_keywords):
            logging.info("✅ %s passed", test_name)
        else:
            logging.warning("❌ %s failed. Expected keywords not found.", test_name)
            failed_tests.append(test_name)
    except Exception as e:
        logging.error("Exception during [%s]: %s", test_name, e)
        failed_tests.append(test_name)


def extract_graph_ids(path):
    """Extract node IDs and edge pairs from the graph data file."""
    node_ids = set()
    edge_pairs = []
    with open(path, "r",  encoding="utf-8") as f:
        for line in f:
            try:
                entry = json.loads(line.strip())
                src = int(entry["source"]["id"])
                dst = int(entry["destination"]["id"])
                src_label = entry["source"]["properties"]["label"]
                dst_label = entry["destination"]["properties"]["label"]
                relationship_type = entry["properties"]["type"]
                relationship_type_id = entry["properties"]["id"]
                relationship_description = entry["properties"]["description"]

                node_ids.update([(src, src_label)])
                node_ids.update([(dst, dst_label)])
                edge_pairs.append(
                    (
                        (src, src_label),
                        (dst, dst_label),
                        relationship_type,
                        relationship_type_id,
                        relationship_description,
                    )
                )
            except Exception as e:
                logging.error("Parsing error: %s", e)
    return list(node_ids), edge_pairs


def test_graph_validation(graph_source, graph_id):
    """Validate the uploaded graph by sending queries and checking responses."""
    node_ids, edge_pairs = extract_graph_ids(graph_source)
    sample_nodes = random.sample(node_ids, min(10, len(node_ids)))
    sample_edges = random.sample(edge_pairs, min(10, len(edge_pairs)))

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((HOST, PORT))

        # Node validation queries
        for nid, label in sample_nodes:
            query = f"MATCH(n:{label}) WHERE n.id={nid} RETURN n.id"
            expected = [f'"n.id":"{nid}"'.encode()]
            send_query(sock, f'cypher-node-%{nid}'  , query, expected, graph_id)

        # Edge validation queries
        for (
                n1,
                n2,
                relationship_type,
                relationship_type_id,
                relationship_description,
        ) in sample_edges:
            query = (
                f"MATCH(n:{n1[1]})-[r:{relationship_type}]-(m:{n2[1]}) "
                f"WHERE n.id={n1[0]} AND m.id={n2[0]} RETURN m.id, r, n.id"
            )
            expected = [
                f'"m.id":"{n2[0]}"'.encode(),
                (
                    f'description":"{relationship_description}",'
                    f'"id":"{relationship_type_id}",'
                    f'"type":"{relationship_type}"'
                ).encode(),
                f'"n.id":"{n1[0]}"'.encode(),
            ]
            send_query(sock, f'cypher-edge-{n1}-{n2}', query, expected, graph_id)

    # Final summary
    print("\n======= Test Summary =======")
    if not failed_tests:
        logging.info("✅ All queries validated successfully.")
    else:
        logging.warning("❌ Some queries failed:")
        for test in failed_tests:
            print(" -", test)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        graph_source = sys.argv[1]
        graph_id = sys.argv[2] if len(sys.argv) > 2 else GRAPH_ID
    else:
        graph_source = "../../env_init/data/graph_with_properties_test2.txt"
        graph_id = GRAPH_ID
    test_graph_validation(graph_source, graph_id)
