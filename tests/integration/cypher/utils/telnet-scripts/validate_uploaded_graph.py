import socket
import logging
import json
import random
import sys
from time import sleep


# Configuration
HOST = '127.0.0.1'
PORT = 7777
GRAPH_ID = '43'
LINE_END = b'\r\n'
CYPHER = b'cypher'

# Logging setup
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
failed_tests = []

def send_query(sock, test_name, query, expected_keywords):
    try:
        # Start CYPHER session for this query
        sock.sendall(CYPHER + LINE_END)
        sleep(0.1)
        sock.recv(1024)  # Expect "Graph ID:"
        sleep(0.1)
        sock.sendall(GRAPH_ID.encode() + LINE_END)
        sock.recv(1024)  # Expect "Input query :"
        sleep(0.1)
        sock.sendall(query.encode() + LINE_END)

        response = b""
        while True:
            chunk = sock.recv(4096)
            if not chunk:
                break
            response += chunk
            if b'done' in response.lower():
                break

        logging.info(f"\n[{test_name}] Query: {query}\nResponse:{response.strip()}\n")

        if all(keyword in response for keyword in expected_keywords):
            logging.info(f"✅ {test_name} passed")
        else:
            logging.warning(f"❌ {test_name} failed. Expected keywords not found.")
            failed_tests.append(test_name)
    except Exception as e:
        logging.error(f"Exception during [{test_name}]: {e}")
        failed_tests.append(test_name)

def extract_graph_ids(path):
    node_ids = set()
    edge_pairs = []
    with open(path, 'r') as f:
        for line in f:
            try:
                entry = json.loads(line.strip())
                src = int(entry['source']['id'])
                dst = int(entry['destination']['id'])
                relationship_type = entry['properties']['type']
                relationship_type_id = entry['properties']['id']
                relationship_description = entry['properties']['description']

                node_ids.update([src, dst])
                edge_pairs.append((src, dst, relationship_type, relationship_type_id, relationship_description))
            except Exception as e:
                logging.error(f"Parsing error: {e}")
    return list(node_ids), edge_pairs

def test_graph_validation():
    node_ids, edge_pairs = extract_graph_ids('../../../env_init/data/graph_data_0.0001GB.txt')
    sample_nodes = random.sample(node_ids, min(10, len(node_ids)))
    sample_edges = random.sample(edge_pairs, min(302, len(edge_pairs)))

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((HOST, PORT))

        # # Node validation queries
        # for nid in sample_nodes:
        #     query = f"MATCH(n) WHERE n.id={nid} RETURN n.id"
        #     expected = [f'"n.id":"{nid}"'.encode()]
        #     send_query(sock, f'cypher-node-{nid}', query, expected)

        # Edge validation queries
        for n1, n2 , relationship_type, relationship_type_id, relationship_description in sample_edges:
            query = f"MATCH(n)-[r]-(m) WHERE n.id={n1} AND m.id={n2} RETURN m.id, r, n.id"
            expected = [f'"m.id":"{n2}"'.encode(),
                        f'description":"{relationship_description}",'
                        f'"id":"{relationship_type_id}",'
                        f'"type":"{relationship_type}"'.encode() ,
                        f'"n.id":"{n1}"'.encode()]
            send_query(sock, f'cypher-edge-{n1}-{n2}', query, expected)

    # Final summary
    print("\n======= Test Summary =======")
    if not failed_tests:
        logging.info("✅ All queries validated successfully.")
    else:
        logging.warning("❌ Some queries failed:")
        for test in failed_tests:
            print(" -", test)

if __name__ == '__main__':
    test_graph_validation()
