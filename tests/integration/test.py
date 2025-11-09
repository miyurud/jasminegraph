"""Copyright 2023 JasmineGraph Team
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

import sys
import socket
import logging
import os
import time

from graphRAG.KG.test import test_KG
from utils.telnetScripts.validate_uploaded_graph import  test_graph_validation

logging.addLevelName(
    logging.INFO, f'\033[1;32m{logging.getLevelName(logging.INFO)}\033[1;0m')
logging.addLevelName(
    logging.WARNING, f'\033[1;33m{logging.getLevelName(logging.WARNING)}\033[1;0m')
logging.addLevelName(
    logging.ERROR, f'\033[1;31m{logging.getLevelName(logging.ERROR)}\033[1;0m')
logging.addLevelName(
    logging.CRITICAL, f'\033[1;41m{logging.getLevelName(logging.CRITICAL)}\033[1;0m')

logging.getLogger().setLevel(logging.INFO)

HOST = '127.0.0.1'
PORT = 7777  # The port used by the server
UI_PORT = 7776 # The port used by the frontend-ui

LIST = b'lst'
ADGR = b'adgr'
ADGR_CUST = b'adgr-cust'
EMPTY = b'empty'
RMGR = b'rmgr'
VCNT = b'vcnt'
ECNT = b'ecnt'
MERGE = b'merge'
TRAIN = b'train'
TRIAN = b'trian'
PGRNK = b'pgrnk'
SHDN = b'shdn'
SEND = b'send'
DONE = b'done'
ADHDFS = b'adhdfs'
LINE_END = b'\r\n'
CYPHER = b'cypher'
UPLOAD_SCRIPT = "utils/datasets/upload-hdfs-file.sh"
OLLAMA_SETUP_SCRIPT = "utils/start-ollama.sh"
TEXT_FOLDER = "graphRAG/KG/gold"
def expect_response(conn: socket.socket, expected: bytes, timeout: float = 30000.0):
    """Check if the response is equal to the expected response within a timeout.
    Return True if they are equal, False otherwise.
    """
    global passed_all
    buffer = bytearray()
    read = 0
    expected_len = len(expected)

    deadline = time.time() + timeout  # set overall timeout deadline

    while read < expected_len:
        # check deadline
        if time.time() > deadline:
            logging.warning('Timed out waiting for full response')
            passed_all = False
            return False

        try:
            received = conn.recv(expected_len - read)
        except socket.error as e:
            logging.warning('Socket error: %s', e)
            passed_all = False
            return False

        if not received:
            logging.warning('Connection closed before expected response was fully received')
            passed_all = False
            return False

        received_len = len(received)
        if received != expected[read:read + received_len]:
            buffer.extend(received)
            data = bytes(buffer)
            logging.warning(
                'Output mismatch\nexpected : %s\nreceived : %s',
                expected.decode(), data.decode())
            passed_all = False
            return False

        read += received_len
        buffer.extend(received)

    data = bytes(buffer)
    print(data.decode('utf-8'), end='')
    assert data == expected
    return True


def expect_response_file(conn: socket.socket, expected: bytes, timeout=5000):
    """Check if the response matches expected file."""
    global passed_all
    buffer = bytearray()
    conn.setblocking(False)
    start = time.time()

    while time.time() - start < timeout:
        try:
            received = conn.recv(4096)
            if received:
                buffer.extend(received)
                start = time.time()
                if b'done' in buffer:
                    break
            else:
                time.sleep(0.01)
        except BlockingIOError:
            time.sleep(0.01)

    conn.setblocking(True)
    data = bytes(buffer)

    received_lines = data.decode(errors='replace').splitlines()
    expected_lines = expected.decode(errors='replace').splitlines()

    mismatches = []
    for i, (exp_line, rec_line) in enumerate(zip(expected_lines, received_lines), start=1):
        if exp_line != rec_line:
            mismatches.append(f'Line {i}:\n  expected: {exp_line}\n  received: {rec_line}')

    # Handle extra lines if lengths differ
    if len(received_lines) > len(expected_lines):
        for i in range(len(expected_lines) + 1, len(received_lines) + 1):
            mismatches.append(f'Line {i}:\n  expected: <no line>\n  '
                              f'received: {received_lines[i-1]}')
        logging.warning('Output mismatch! Showing first 10 differences:\n%s',
            '\n'.join(mismatches[:10]))
        passed_all = False
        return False
    if len(expected_lines) > len(received_lines):
        for i in range(len(received_lines) + 1, len(expected_lines) + 1):
            mismatches.append(f'Line {i}:\n  expected: {expected_lines[i-1]}\n'
                              f'  received: <no line>')
        logging.warning('Output mismatch! Showing first 10 differences:\n%s',
            '\n'.join(mismatches[:10]))
        passed_all = False
        return False

    print('All the records match')
    return True

def send_and_expect_response(conn, test_name, send, expected, exit_on_failure=False):
    """Send a message to server and check if the response is equal to the expected response
    Append the test name to failed tests list on failure.
    If exit_on_failure is True, and the response did not match, exit the test script after printing
    the test stats.
    """
    conn.sendall(send + LINE_END)
    print(send.decode('utf-8'))
    if not expect_response(conn, expected + LINE_END):
        failed_tests.append(test_name)
        if exit_on_failure:
            print()
            logging.fatal('Failed some tests,')
            print(*failed_tests, sep='\n', file=sys.stderr)
            sys.exit(1)

def send_and_expect_response_file(conn, test_name, send, expected_file, exit_on_failure=False):
    """Send a message to server and check the response on-the-fly against a large expected
    response file."""
    conn.sendall(send + LINE_END)
    print(send.decode('utf-8'))
    with open(expected_file, 'rb') as f:
        expected_bytes = f.read()

    if not expect_response_file(conn, expected_bytes):
        failed_tests.append(test_name)
        if exit_on_failure:
            print()
            logging.fatal('Failed some tests,')
            print(*failed_tests, sep='\n', file=sys.stderr)
            sys.exit(1)


passed_all = True
failed_tests = []

def test(host, port):
    """Test the JasmineGraph server by sending a series of commands and checking the responses."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((host, port))
        # print()
        # logging.info('Testing lst')
        # send_and_expect_response(sock, 'Initial lst', LIST, EMPTY)
        #
        # print()
        # logging.info('Testing adgr')
        # send_and_expect_response(sock, 'adgr', ADGR, SEND, exit_on_failure=True)
        # send_and_expect_response(
        # sock, 'adgr', b'powergrid|/var/tmp/data/powergrid.dl', DONE, exit_on_failure=True)
        #
        # print()
        # logging.info('Testing lst after adgr')
        # send_and_expect_response(sock, 'lst after adgr', LIST,
        #                          b'|1|powergrid|/var/tmp/data/powergrid.dl|op|')
        #
        # print()
        # logging.info('Testing ecnt')
        # send_and_expect_response(sock, 'ecnt', ECNT, b'graphid-send')
        # send_and_expect_response(sock, 'ecnt', b'1', b'6594')
        #
        # print()
        # logging.info('Testing vcnt')
        # send_and_expect_response(sock, 'vcnt', VCNT, b'graphid-send')
        # send_and_expect_response(sock, 'vcnt', b'1', b'4941')
        #
        # print()
        # logging.info('Testing trian')
        # send_and_expect_response(sock, 'trian', TRIAN,
        #                          b'graphid-send', exit_on_failure=True)
        # send_and_expect_response(
        #     sock, 'trian', b'1', b'priority(>=1)', exit_on_failure=True)
        # send_and_expect_response(sock, 'trian', b'1', b'651')
        #
        # print()
        # logging.info('Testing pgrnk')
        # send_and_expect_response(sock, 'pgrnk', PGRNK,
        #                          b'grap', exit_on_failure=True)
        # send_and_expect_response(
        #     sock, 'pgrnk', b'1|0.5|40', b'priority(>=1)', exit_on_failure=True)
        # send_and_expect_response(sock, 'pgrnk', b'1',
        #                          DONE, exit_on_failure=True)
        #
        # print()
        # logging.info('Testing adgr-cust')
        # send_and_expect_response(sock, 'adgr-cust', ADGR_CUST,
        #                          b'Select a custom graph upload option' + LINE_END +
        #                          b'1 : Graph with edge list + text attributes list' + LINE_END +
        #                          b'2 : Graph with edge list + JSON attributes list' + LINE_END +
        #                          b'3 : Graph with edge list + XML attributes list',
        #                          exit_on_failure=True)
        # send_and_expect_response(sock, 'adgr-cust',
        #                          b'1',
        #                          b'Send <name>|<path to edge list>|<path to attribute file>|' +
        #                          b'(optional)<attribute data type: int8. int16, int32 or float>',
        #                          exit_on_failure=True)
        # send_and_expect_response(sock, 'adgr-cust',
        #                          b'cora|/var/tmp/data/cora/cora.cites|' +
        #                          b'/var/tmp/data/cora/cora.content',
        #                          DONE, exit_on_failure=True)
        #
        # print()
        # logging.info('Testing lst after adgr-cust')
        # send_and_expect_response(sock, 'lst after adgr-cust', LIST,
        #                          b'|1|powergrid|/var/tmp/data/powergrid.dl|op|' + LINE_END +
        #                          b'|2|cora|/var/tmp/data/cora/cora.cites|op|')
        #
        # # print()
        # # logging.info('Testing merge')
        # # send_and_expect_response(sock, 'merge', MERGE, b'Available main flags:' + LINE_END +
        # #                          b'graph_id' + LINE_END +
        # #                          b'Send --<flag1> <value1>')
        # # send_and_expect_response(
        # #     sock, 'merge', b'--graph_id 2', DONE, exit_on_failure=True)
        #
        # print()
        # logging.info('Testing train')
        # send_and_expect_response(sock, 'train', TRAIN, b'Available main flags:' + LINE_END +
        #                          b'graph_id learning_rate batch_size validate_iter epochs' +
        #                          LINE_END + b'Send --<flag1> <value1> --<flag2> <value2> ..',
        #                          exit_on_failure=True)
        # send_and_expect_response(
        #     sock, 'train', b'--graph_id 2', DONE, exit_on_failure=True)
        #
        # print()
        # logging.info('Testing rmgr')
        # send_and_expect_response(sock, 'rmgr', RMGR, SEND)
        # send_and_expect_response(sock, 'rmgr', b'2', DONE)
        #
        # print()
        # logging.info('Testing lst after rmgr')
        # send_and_expect_response(sock, 'lst after rmgr',
        #                          LIST, b'|1|powergrid|/var/tmp/data/powergrid.dl|op|')
        #
        # send_and_expect_response(sock, 'rmgr', RMGR, SEND)
        # send_and_expect_response(sock, 'rmgr', b'1', DONE)
        #
        # # Test cases for hdfs implementation for custom hdfs server
        # print()
        # logging.info('Testing adhdfs for custom HDFS server')
        # send_and_expect_response(sock, 'adhdfs', ADHDFS,
        #                          b'Do you want to use the default HDFS server(y/n)?',
        #                          exit_on_failure=True)
        # send_and_expect_response(sock, 'adhdfs', b'n',
        #                          b'Send the file path to the HDFS configuration file.' +
        #                          b' This file needs to be in some directory location ' +
        #                          b'that is accessible for JasmineGraph master',
        #                          exit_on_failure=True)
        # send_and_expect_response(sock, 'adhdfs', b'/var/tmp/config/hdfs_config.txt',
        #                          b'HDFS file path: ',
        #                          exit_on_failure=True)
        # send_and_expect_response(sock, 'adhdfs', b'/home/powergrid.dl',
        #                          b'Is this an edge list type graph(y/n)?',
        #                          exit_on_failure=True)
        # send_and_expect_response(sock, 'adhdfs', b'y',
        #                          b'Is this a directed graph(y/n)?',
        #                          exit_on_failure=True)
        # send_and_expect_response(sock, 'adhdfs', b'y', DONE, exit_on_failure=True)
        #
        # print()
        # logging.info('Testing lst after adhdfs')
        # send_and_expect_response(sock, 'lst after adhdfs', LIST,
        #                          b'|1|/home/powergrid.dl|hdfs:/home/powergrid.dl|op|',
        #                          exit_on_failure=True)
        #
        # # print()
        # # logging.info('1. Testing ecnt after adhdfs')
        # # send_and_expect_response(sock, 'ecnt', ECNT, b'graphid-send', exit_on_failure=True)
        # # send_and_expect_response(sock, 'ecnt', b'1', b'6594', exit_on_failure=True)
        #
        # # print()
        # # logging.info('1. Testing vcnt after adhdfs')
        # # send_and_expect_response(sock, 'vcnt', VCNT, b'graphid-send', exit_on_failure=True)
        # # send_and_expect_response(sock, 'vcnt', b'1', b'4941', exit_on_failure=True)
        #
        # print()
        # logging.info('Testing adhdfs for custom graph with properties')
        # send_and_expect_response(sock, 'adhdfs', ADHDFS,
        #                          b'Do you want to use the default HDFS server(y/n)?',
        #                          exit_on_failure=True)
        # send_and_expect_response(sock, 'adhdfs', b'n',
        #                          b'Send the file path to the HDFS configuration file.' +
        #                          b' This file needs to be in some directory location ' +
        #                          b'that is accessible for JasmineGraph master',
        #                          exit_on_failure=True)
        # send_and_expect_response(sock, 'adhdfs', b'/var/tmp/config/hdfs_config.txt',
        #                          b'HDFS file path: ',
        #                          exit_on_failure=True)
        # send_and_expect_response(sock, 'adhdfs', b'/home/graph_with_properties.txt',
        #                          b'Is this an edge list type graph(y/n)?',
        #                          exit_on_failure=True)
        # send_and_expect_response(sock, 'adhdfs', b'n',
        #                          b'Is this a directed graph(y/n)?',
        #                          exit_on_failure=True)
        # send_and_expect_response(sock, 'adhdfs', b'y', DONE, exit_on_failure=True)
        #
        #
        # print()
        # logging.info('2. Testing cypher aggregate query after adding the graph')
        # send_and_expect_response(sock, 'cypher', CYPHER, b'Graph ID:', exit_on_failure=True)
        # # send_and_expect_response(sock, 'cypher', CYPHER, b'Graph ID:', exit_on_failure=True)
        # send_and_expect_response(sock, 'cypher', b'2', b'Input query :', exit_on_failure=True)
        # send_and_expect_response(sock, 'cypher', b'match (n) where n.id < 10 return avg(n.id)',
        #                          b'{"avg(n.id)":4.5}', exit_on_failure=True)
        # send_and_expect_response(sock, 'cypher', b'',
        #                          b'done', exit_on_failure=True)
        #
        # print()
        # logging.info('[Cypher] Uploading graph for cypher testing')
        # send_and_expect_response(sock, 'adhdfs', ADHDFS,
        #                          b'Do you want to use the default HDFS server(y/n)?',
        #                          exit_on_failure=True)
        # send_and_expect_response(sock, 'adhdfs', b'n',
        #                          b'Send the file path to the HDFS configuration file.' +
        #                          b' This file needs to be in some directory location ' +
        #                          b'that is accessible for JasmineGraph master',
        #                          exit_on_failure=True)
        # send_and_expect_response(sock, 'adhdfs', b'/var/tmp/config/hdfs_config.txt',
        #                          b'HDFS file path: ',
        #                          exit_on_failure=True)
        # send_and_expect_response(sock, 'adhdfs', b'/home/graph_with_properties.txt',
        #                          b'Is this an edge list type graph(y/n)?',
        #                          exit_on_failure=True)
        # send_and_expect_response(sock, 'adhdfs', b'n',
        #                          b'Is this a directed graph(y/n)?',
        #                          exit_on_failure=True)
        # send_and_expect_response(sock, 'adhdfs', b'y', DONE, exit_on_failure=True)
        #
        # print()
        # logging.info('[Cypher] Uploading large graph for cypher testing')
        # send_and_expect_response(sock, 'adhdfs', ADHDFS,
        #                          b'Do you want to use the default HDFS server(y/n)?',
        #                          exit_on_failure=True)
        # send_and_expect_response(sock, 'adhdfs', b'n',
        #                          b'Send the file path to the HDFS configuration file.' +
        #                          b' This file needs to be in some directory location ' +
        #                          b'that is accessible for JasmineGraph master',
        #                          exit_on_failure=True)
        # send_and_expect_response(sock, 'adhdfs', b'/var/tmp/config/hdfs_config.txt',
        #                          b'HDFS file path: ',
        #                          exit_on_failure=True)
        # send_and_expect_response(sock, 'adhdfs', b'/home/graph_with_properties_large.txt',
        #                          b'Is this an edge list type graph(y/n)?',
        #                          exit_on_failure=True)
        # send_and_expect_response(sock, 'adhdfs', b'n',
        #                          b'Is this a directed graph(y/n)?',
        #                          exit_on_failure=True)
        # send_and_expect_response(sock, 'adhdfs', b'y', DONE, exit_on_failure=True)
        #
        # print()
        # logging.info('[Adhdfs] Testing uploaded graph')
        # abs_path = os.path.abspath('tests/integration/env_init/data/graph_with_properties.txt')
        # test_graph_validation(abs_path, '2' ,host, port)
        #
        # print()
        # logging.info('[Cypher] Testing AllNodeScan ')
        # send_and_expect_response(sock, 'cypher', CYPHER, b'Graph ID:', exit_on_failure=True)
        # send_and_expect_response(sock, 'cypher', b'2', b'Input query :', exit_on_failure=True)
        # send_and_expect_response(sock, 'cypher', b'MATCH (n) WHERE n.id=2 RETURN n ',
        #                          b'{"n":{"id":"2","label":"Person","name":"Charlie",'
        #                          b'"occupation":"IT Engineer",'
        #                          b'"partitionID":"0"}}', exit_on_failure=True)
        # send_and_expect_response(sock, 'cypher', b'',
        #                          b'done', exit_on_failure=True)
        #
        # print()
        # logging.info('[Cypher] Testing ProduceResults ')
        # send_and_expect_response(sock, 'cypher', CYPHER, b'Graph ID:', exit_on_failure=True)
        # send_and_expect_response(sock, 'cypher', b'2', b'Input query :', exit_on_failure=True)
        # send_and_expect_response(sock, 'cypher', b'MATCH (n) WHERE n.id = 18 RETURN n.age, n.name ',
        #                          b'{"n.age":null,"n.name":"Skyport Airport"}',
        #                          exit_on_failure=True)
        # send_and_expect_response(sock, 'cypher', b'',
        #                          b'done', exit_on_failure=True)
        #
        # print()
        # logging.info('[Cypher] Testing ProduceResults')
        # send_and_expect_response(sock, 'cypher', CYPHER, b'Graph ID:', exit_on_failure=True)
        # send_and_expect_response(sock, 'cypher', b'2', b'Input query :', exit_on_failure=True)
        # send_and_expect_response(sock, 'cypher', b'MATCH (n) WHERE n.id = 18 RETURN n.age, n.name ',
        #                          b'{"n.age":null,"n.name":"Skyport Airport"}',
        #                          exit_on_failure=True)
        # send_and_expect_response(sock, 'cypher', b'',
        #                          b'done', exit_on_failure=True)
        #
        # print()
        # logging.info('[Cypher] Testing filter by equality check')
        # send_and_expect_response(sock, 'cypher', CYPHER, b'Graph ID:', exit_on_failure=True)
        # send_and_expect_response(sock, 'cypher', b'2', b'Input query :', exit_on_failure=True)
        # send_and_expect_response(sock, 'cypher', b"MATCH (n) WHERE n.name = 'Fiona' RETURN n",
        #                          b'{"n":{"age":"25","id":"10","label":"Person",'
        #                          b'"name":"Fiona","occupation":"Artist",'
        #                          b'"partitionID":"0"}}',
        #                          exit_on_failure=True)
        # send_and_expect_response(sock, 'cypher', b'',
        #                          b'done', exit_on_failure=True)
        #
        # print()
        # logging.info('[Cypher] Testing filter by comparison of integer attribute')
        # send_and_expect_response(sock, 'cypher', CYPHER, b'Graph ID:', exit_on_failure=True)
        # send_and_expect_response(sock, 'cypher', b'2', b'Input query :', exit_on_failure=True)
        # send_and_expect_response(sock, 'cypher', b'MATCH (n) WHERE n.age < 30 return n',
        #                          b'{"n":{"age":"25","id":"10","label":"Person",'
        #                          b'"name":"Fiona","occupation":"Artist",'
        #                          b'"partitionID":"0"}}',
        #                          exit_on_failure=True)
        # send_and_expect_response(sock, 'cypher', b'',
        #                          b'done', exit_on_failure=True)
        #
        #
        # print()
        # logging.info('[Cypher] Testing expand all ')
        # send_and_expect_response(sock, 'cypher', CYPHER, b'Graph ID:', exit_on_failure=True)
        # send_and_expect_response(sock, 'cypher', b'2', b'Input query :', exit_on_failure=True)
        # send_and_expect_response(sock, 'cypher',b'MATCH (a)-[r]-(b)-[d]-(s)'
        #                                         b' WHERE (a.id = 10 AND s.id=14) RETURN a, b, s',
        #                          b'{"a":{"age":"25","id":"10","label":"Person",'
        #                          b'"name":"Fiona","occupation":"Artist","partitionID":"0"},'
        #                          b'"b":{"id":"2","label":"Person","name":"Charlie",'
        #                          b'"occupation":"IT Engineer","partitionID":"0"},'
        #                          b'"s":{"id":"14","label":"Person",'
        #                          b'"name":"Julia","occupation":"Entrepreneur","partitionID":"0"}}',
        #                          exit_on_failure=True)
        #
        # send_and_expect_response(sock, 'cypher', b'',
        #                          b'done', exit_on_failure=True)
        # print()
        # logging.info('[Cypher] Testing Undirected Relationship Type Scan')
        # send_and_expect_response(sock, 'cypher', CYPHER, b'Graph ID:', exit_on_failure=True)
        # send_and_expect_response(sock, 'cypher', b'2', b'Input query :', exit_on_failure=True)
        # send_and_expect_response(sock, 'cypher',b'MATCH '
        #                                         b"(n {name:'Eva'})-[:NEIGHBORS]-(x ) RETURN x",
        #
        #                          b'{"x":{"id":"0","label":"Person","name":"Alice",'
        #                          b'"occupation":"Teacher","partitionID":"0"}}',
        #                          exit_on_failure=True)
        # send_and_expect_response(sock, 'cypher', b'',
        #                          b'done', exit_on_failure=True)
        #
        #
        # print()
        # logging.info('[Cypher] Testing Undirected All Relationship Scan')
        # send_and_expect_response(sock, 'cypher', CYPHER, b'Graph ID:', exit_on_failure=True)
        # send_and_expect_response(sock, 'cypher', b'2', b'Input query :', exit_on_failure=True)
        # send_and_expect_response(sock, 'cypher',b'MATCH (n)-[r]-(m {id:6} ) WHERE n.age = 25'
        #                                         b' RETURN n, r, m',
        #                          b'{"m":{"category":"Park","id":"6","label":"Location",'
        #                          b'"name":"Central Park",'
        #                          b'"partitionID":"0"},"n":{"age":"25","id":"10","label":"Person",'
        #                          b'"name":"Fiona","occupation":"Artist","partitionID":"0"'
        #                          b'},"r":{"description":"Fiona and Central Park have'
        #                          b' been friends since college.","id":"11",'
        #                          b'"type":"FRIENDS"}}',
        #
        #                          exit_on_failure=True)
        # send_and_expect_response(sock, 'cypher', b'',
        #                          b'done', exit_on_failure=True)
        #
        # print()
        # logging.info('[Cypher] Testing Directed Relationship Type Scan ')
        # send_and_expect_response(sock, 'cypher', CYPHER, b'Graph ID:', exit_on_failure=True)
        # send_and_expect_response(sock, 'cypher', b'2', b'Input query :',
        #                          exit_on_failure=True)
        # send_and_expect_response(sock, 'cypher',b'MATCH'
        #                                         b" (n {name:'Eva'})-[:NEIGHBORS]->(x ) RETURN x",
        #
        #                          b'{"x":{"id":"0","label":"Person","name":"Alice",'
        #                          b'"occupation":"Teacher","partitionID":"0"}}',
        #                          exit_on_failure=True)
        # send_and_expect_response(sock, 'cypher', b'',
        #                          b'done', exit_on_failure=True)
        #
        # print()
        # logging.info('[Cypher] Testing OrderBy ')
        # send_and_expect_response(sock, 'cypher', CYPHER, b'Graph ID:', exit_on_failure=True)
        # send_and_expect_response(sock, 'cypher', b'2', b'Input query :', exit_on_failure=True)
        # send_and_expect_response(sock, 'cypher',b"match (n) where n.partitionID = '1' return n "
        #                                         b'order by n.name ASC',
        #                          b'''{"n":{"category":"Studio","id":"15","label":"Location",'''
        #                          b'''"name":"Art Studio","partitionID":"1"}}''',
        #                          exit_on_failure=True)
        # send_and_expect_response(sock, 'cypher', b'',
        #                          b'{"n":{"id":"1","label":"Person","name":"Bob","occupation":'
        #                          b'"Banker","partitionID":"1"}}', exit_on_failure=True)
        #
        # send_and_expect_response(sock, 'cypher', b'',
        #                          b'{"n":{"id":"3","label":"Person","name":"David","occupation":'
        #                          b'"Doctor","partitionID":"1"}}', exit_on_failure=True)
        #
        # send_and_expect_response(sock, 'cypher', b'',b'{"n":{"id":"11","label":"Person",'
        #                                              b'"name":"George","occupation":"Chef",'
        #                                              b'"partitionID":"1"}}', exit_on_failure=True)
        #
        # send_and_expect_response(sock, 'cypher', b'',
        #                          b'{"n":{"category":"Restaurant","id":"17","label":"Location",'
        #                          b'"name":"Gourmet Bistro","partitionID":"1"}}',
        #                          exit_on_failure=True)
        #
        # send_and_expect_response(sock, 'cypher', b'',
        #                          b'{"n":{"category":"School","id":"5","label":"Location",'
        #                          b'"name":"Greenfield School","partitionID":"1"}}',
        #                          exit_on_failure=True)
        #
        # send_and_expect_response(sock, 'cypher', b'',b'{"n":{"id":"13","label":"Person",'
        #                                              b'"name":"Ian","occupation":"Pilot",'
        #                                              b'"partitionID":"1"}}', exit_on_failure=True)
        #
        # send_and_expect_response(sock, 'cypher', b'',
        #                          b'{"n":{"category":"Coworking Space","id":"19","label":'
        #                          b'"Location","name":"Innovation Hub","partitionID":"1"}}',
        #                          exit_on_failure=True)
        #
        # send_and_expect_response(sock, 'cypher', b'',
        #                          b'{"n":{"category":"Bank","id":"7","label":"Location","name":'
        #                          b'"Town Bank","partitionID":"1"}}', exit_on_failure=True)
        #
        # send_and_expect_response(sock, 'cypher', b'',
        #                          b'{"n":{"category":"Hospital","id":"9","label":"Location",'
        #                          b'"name":"Town General Hospital","partitionID":"1"}}',
        #                          exit_on_failure=True)
        # send_and_expect_response(sock, 'cypher', b'',
        #                          b'done', exit_on_failure=True)
        #
        # print()
        # logging.info('[Cypher] Testing Node Scan By Label')
        # send_and_expect_response(sock, 'cypher', CYPHER, b'Graph ID:', exit_on_failure=True)
        # send_and_expect_response(sock, 'cypher', b'2', b'Input query :', exit_on_failure=True)
        # send_and_expect_response(sock, 'cypher',b'match(n:Person) where n.id=2 return n'
        #                                         b' RETURN n',b'{"n":{"id":"2","label":"Person",'
        #                                         b'"name":"Charlie","occupation":"IT Engineer",'
        #                                         b'"partitionID":"0"}}',
        #
        #                          exit_on_failure=True)
        # send_and_expect_response(sock, 'cypher', b'',
        #                          b'done', exit_on_failure=True)
        #
        #

        print()
        test_KG(OLLAMA_SETUP_SCRIPT , TEXT_FOLDER ,UPLOAD_SCRIPT)




        logging.info('[Cypher] Testing rmgr after adhdfs')
        send_and_expect_response(sock, 'rmgr', RMGR, SEND, exit_on_failure=True)
        send_and_expect_response(sock, 'rmgr', b'1', DONE, exit_on_failure=True)
        print()
        logging.info('Testing rmgr after adhdfs')
        send_and_expect_response(sock, 'rmgr', RMGR, SEND, exit_on_failure=True)
        send_and_expect_response(sock, 'rmgr', b'2', DONE, exit_on_failure=True)
        send_and_expect_response(sock, 'rmgr', RMGR, SEND, exit_on_failure=True)
        send_and_expect_response(sock, 'rmgr', b'3', DONE, exit_on_failure=True)
        print()
        logging.info('[Cypher] Testing OrderBy for Large Graph')
        send_and_expect_response(sock, 'cypher', CYPHER, b'Graph ID:', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', b'4', b'Input query :', exit_on_failure=True)
        send_and_expect_response_file(sock,'cypher', b'MATCH (n) RETURN n.id, n.name, n.code '
                                                     b'ORDER BY n.code ASC',
                                      'tests/integration/utils/expected_output/'
                                      'orderby_expected_output_file.txt',exit_on_failure=True)

        # shutting down workers after testing
        print()
        logging.info('Shutting down')
        sock.sendall(SHDN + LINE_END)
        if passed_all:
            print()
            logging.info('Passed all tests')
        else:
            print()
            logging.critical('Failed some tests')
            print(*failed_tests, sep='\n', file=sys.stderr)
            sys.exit(1)

if __name__ == '__main__':
    test(HOST, PORT)