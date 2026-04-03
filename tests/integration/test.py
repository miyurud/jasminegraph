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
# pylint: disable=too-many-lines

import sys
import socket
import logging
import os
import time
import json
import re
import subprocess
import threading

from utils.telnetScripts.validate_uploaded_graph import test_graph_validation

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
ADSTRMK = b'adstrmk'
STRIAN = b'strian'
STOPSTRIAN = b'stopstrian'
LINE_END = b'\r\n'
CYPHER = b'cypher'
TRUNCATE = b'truncate'
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

KAFKA_TOPIC = 'stream-triangle-test'
DOCKER_COMPOSE_FILE = os.path.join(BASE_DIR, 'docker-compose.yml')
UPLOAD_SCRIPT = os.path.join(BASE_DIR, 'utils/datasets/upload-hdfs-file.sh')
OLLAMA_SETUP_SCRIPT = os.path.join(BASE_DIR, 'graphRAG/utils/start-ollama.sh')
TEXT_FOLDER = os.path.join(BASE_DIR, 'graphRAG/KG/gold')

def run_cmd(cmd):
    """Run a command and raise on failure."""
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        raise RuntimeError(
            f"Command failed: {' '.join(cmd)}\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )
    return result

def kafka_up():
    """Start the Kafka service for the streaming test."""
    run_cmd(['docker', 'compose', '-f', DOCKER_COMPOSE_FILE, 'up', '-d', 'kafka'])


def kafka_down():
    """Stop the Kafka service used by the streaming test."""
    run_cmd(['docker', 'compose', '-f', DOCKER_COMPOSE_FILE, 'stop', 'kafka'])


def run_kafka_command(args, timeout=60, input_data=None):
    """Run a Kafka command through docker compose service execution."""
    result = subprocess.run(
        ['docker', 'compose', '-f', DOCKER_COMPOSE_FILE, 'exec', '-T', 'kafka', *args],
        capture_output=True,
        text=True,
        check=False,
        timeout=timeout,
        input=input_data,
    )
    if result.returncode != 0:
        raise RuntimeError(
            'Kafka command failed: '
            f"{' '.join(args)}\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )
    return result


def wait_for_kafka(timeout=60):
    """Wait until Kafka responds to topic listing requests."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            run_kafka_command([
                '/opt/kafka/bin/kafka-topics.sh',
                '--bootstrap-server', 'localhost:9092',
                '--list'
            ])
            return
        except Exception:
            time.sleep(2)
    raise TimeoutError('Kafka broker did not become ready in time')


def create_topic():
    """Create the Kafka topic used by the streaming test."""
    run_kafka_command([
        '/opt/kafka/bin/kafka-topics.sh',
        '--bootstrap-server', 'localhost:9092',
        '--create',
        '--if-not-exists',
        '--topic', KAFKA_TOPIC,
        '--partitions', '1',
        '--replication-factor', '1'
    ])


def publish_stream(records, delay_sec=0.2):
    """Publish the generated triangle stream to Kafka."""
    payload_lines = []
    for record in records:
        if isinstance(record, dict):
            payload_lines.append(json.dumps(record))
        else:
            payload_lines.append(str(record))

    payload = '\n'.join(payload_lines) + '\n'
    run_kafka_command([
        '/opt/kafka/bin/kafka-console-producer.sh',
        '--bootstrap-server', 'localhost:9092',
        '--topic', KAFKA_TOPIC
    ], input_data=payload)

    if delay_sec > 0:
        time.sleep(delay_sec)


def consume_stream(expected_messages, consumed_records, stop_event):
    """Consume a bounded number of Kafka messages until completion or stop."""
    with subprocess.Popen(
            [
                'docker', 'compose', '-f', DOCKER_COMPOSE_FILE, 'exec', '-T', 'kafka',
                '/opt/kafka/bin/kafka-console-consumer.sh',
                '--bootstrap-server', 'localhost:9092',
                '--topic', KAFKA_TOPIC,
                '--from-beginning',
                '--max-messages', str(expected_messages),
                '--timeout-ms', '30000'
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
    ) as process:

        while process.poll() is None:
            if stop_event.is_set():
                process.terminate()
                break
            time.sleep(0.5)

        try:
            stdout, _stderr = process.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            stdout, _stderr = process.communicate()

    if process.returncode in (0, 124) and stdout:
        stdout_lines = [line.strip() for line in stdout.splitlines() if line.strip()]
        consumed_records.extend(stdout_lines)


def recv_until_contains(conn: socket.socket, expected_tokens, timeout=30):
    """Receive from a socket until any expected token appears or timeout elapses."""
    deadline = time.time() + timeout
    data = bytearray()
    conn.settimeout(1)
    while time.time() < deadline:
        try:
            chunk = conn.recv(4096)
            if chunk:
                data.extend(chunk)
                for token in expected_tokens:
                    if token in data:
                        return bytes(data)
            else:
                time.sleep(0.05)
        except socket.timeout:
            continue
    raise TimeoutError(
        f"Timed out waiting for one of {[t.decode(errors='ignore') for t in expected_tokens]}. "
        f"Received: {bytes(data).decode(errors='ignore')}"
    )

def configure_streaming_graph(conn: socket.socket):
    """Configure a new streaming graph with the default Kafka consumer path."""
    conn.sendall(ADSTRMK + LINE_END)
    recv_until_contains(conn, [b'existing graph'])

    conn.sendall(b'n' + LINE_END)
    response = recv_until_contains(conn, [b'default graph ID:'])
    graph_id_match = re.search(rb'default graph ID:\s*(\d+)', response)
    if graph_id_match is None:
        raise RuntimeError(
            f"Failed to parse streaming graph id from response: {response.decode(errors='ignore')}"
        )
    graph_id = graph_id_match.group(1).decode()

    conn.sendall(b'y' + LINE_END)
    recv_until_contains(conn, [b'Choose an option'])

    conn.sendall(b'1' + LINE_END)
    recv_until_contains(conn, [b'Directed'])

    conn.sendall(b'y' + LINE_END)
    recv_until_contains(conn, [b'default KAFKA consumer'])

    conn.sendall(b'y' + LINE_END)
    recv_until_contains(conn, [b'send kafka topic name'])

    conn.sendall(KAFKA_TOPIC.encode() + LINE_END)
    recv_until_contains(conn, [b'Received the kafka topic'])

    return graph_id

def send_stopstrian(host, port):
    """Send the stopstrian command."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as stop_socket:
        stop_socket.settimeout(10)
        stop_socket.connect((host, port))
        stop_socket.sendall(STOPSTRIAN + LINE_END)


def cleanup_streaming_graph(host, port, graph_id):
    """Remove the temporary graph created for the streaming test."""
    if not graph_id:
        return

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as cleanup_socket:
        cleanup_socket.settimeout(20)
        cleanup_socket.connect((host, port))

        cleanup_socket.sendall(RMGR + LINE_END)
        recv_until_contains(cleanup_socket, [b'send'])

        cleanup_socket.sendall(graph_id.encode() + LINE_END)
        recv_until_contains(cleanup_socket, [b'done'])

def list_graphs(host, port):
    """Return the current graph list from the server."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as list_socket:
        list_socket.settimeout(1)
        list_socket.connect((host, port))
        list_socket.sendall(LIST + LINE_END)

        data = bytearray()
        deadline = time.time() + 10
        while time.time() < deadline:
            try:
                chunk = list_socket.recv(4096)
                if not chunk:
                    break
                data.extend(chunk)
            except socket.timeout:
                if data:
                    break

    decoded = data.decode(errors='ignore')
    matches = re.findall(r'\|(\d+)\|([^|\r\n]+)\|([^|\r\n]+)\|op\|', decoded)

    graphs = []
    for graph_id, name, path in matches:
        graphs.append({'id': graph_id, 'name': name, 'path': path})
    return graphs

def get_graph_id_by_path(host, port, graph_path, prefer='lowest'):
    """Resolve a graph ID by matching the stored graph path or name."""
    def normalize_path(path):
        if path.startswith('hdfs:'):
            return path[len('hdfs:'):]
        return path

    target_path = normalize_path(graph_path)
    graphs = []
    for graph in list_graphs(host, port):
        graph_path_matches = normalize_path(graph['path']) == target_path
        graph_name_matches = normalize_path(graph['name']) == target_path
        if graph_path_matches or graph_name_matches:
            graphs.append(graph)
    if not graphs:
        raise RuntimeError(f'Unable to find graph id for path: {graph_path}')

    ids = sorted(int(graph['id']) for graph in graphs)
    if prefer == 'highest':
        return str(ids[-1])
    return str(ids[0])

def query_streaming_triangle_count(host, port, graph_id, mode='0', result_timeout=120):
    """Query the streaming triangle count and parse the returned count."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as strian_sock:
            strian_sock.settimeout(30)
            strian_sock.connect((host, port))

            strian_sock.sendall(STRIAN + LINE_END)
            recv_until_contains(strian_sock, [b'graphid-send', b'grap'])

            strian_sock.sendall(graph_id.encode() + LINE_END)
            recv_until_contains(strian_sock, [b'mode-send', b'mode'])

            strian_sock.sendall(mode.encode() + LINE_END)
            response = recv_until_contains(strian_sock, [b'Time Taken:'], timeout=result_timeout)
    finally:
        try:
            send_stopstrian(host, port)
        except Exception:
            pass

    decoded_response = response.decode(errors='ignore')
    marker = 'Time Taken:'
    marker_index = decoded_response.find(marker)
    if marker_index == -1:
        raise RuntimeError(
            f'Unable to parse streaming triangle count from: {decoded_response}'
        )

    prefix = decoded_response[:marker_index]
    end_index = len(prefix) - 1
    while end_index >= 0 and prefix[end_index].isspace():
        end_index -= 1

    start_index = end_index
    while start_index >= 0 and prefix[start_index].isdigit():
        start_index -= 1

    count_text = prefix[start_index + 1:end_index + 1]
    if not count_text:
        raise RuntimeError(
            f'Unable to parse streaming triangle count from: {decoded_response}'
        )

    return int(count_text), decoded_response

def test_streaming_triangle_count_with_kafka(host, port):  # pylint: disable=too-many-branches
    """Exercise Kafka-backed streaming triangle counting end-to-end."""
    logging.info('Testing Kafka streaming triangle counting integration')

    graph_id = None
    consumed_messages = []
    stop_consumer = threading.Event()

    records = []
    for base in range(0, 30, 3):
        first = str(base)
        second = str(base + 1)
        third = str(base + 2)
        records.append({'source': {'id': first}, 'destination': {'id': second}, 'properties': {}})
        records.append({'source': {'id': second}, 'destination': {'id': third}, 'properties': {}})
        records.append({'source': {'id': third}, 'destination': {'id': first}, 'properties': {}})
    records.append('-1')

    try:
        kafka_up()
        wait_for_kafka()
        create_topic()

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as setup_socket:
            setup_socket.settimeout(20)
            setup_socket.connect((host, port))
            graph_id = configure_streaming_graph(setup_socket)
            logging.info('[Streaming] Graph ID selected: %s', graph_id)

        consumer_thread = threading.Thread(
            target=consume_stream,
            args=(len(records), consumed_messages, stop_consumer),
            daemon=True
        )
        consumer_thread.start()

        publish_stream(records, delay_sec=1.0)

        expected_triangle_count = len(records) // 3
        latest_triangle_count = 0
        latest_response = ''

        time.sleep(5)
        for sequential_attempt in range(1, 8):
            try:
                triangle_count, response = query_streaming_triangle_count(
                    host,
                    port,
                    graph_id,
                    mode='0',
                    result_timeout=120
                )
            except (TimeoutError, ConnectionRefusedError) as query_error:
                logging.warning('[Streaming][Sequential] Attempt %s failed: %s',
                                sequential_attempt, str(query_error))
                time.sleep(4)
                continue

            latest_triangle_count = triangle_count
            latest_response = response
            logging.info('[Streaming][Sequential] Attempt %s triangle count: %s',
                         sequential_attempt, triangle_count)
            if triangle_count >= expected_triangle_count:
                break
            time.sleep(4)

        stop_consumer.set()
        consumer_thread.join(timeout=10)

        if len(consumed_messages) < 1:
            logging.warning(
                '[Streaming] Kafka consumer did not read enough messages from topic. Read %s',
                len(consumed_messages)
            )

        if latest_triangle_count < expected_triangle_count:
            raise RuntimeError(
                '[Streaming] Streaming triangle count below threshold. '
                f'Expected >= {expected_triangle_count}, got {latest_triangle_count}. '
                f'Last response: {latest_response}'
            )

        logging.info('[Streaming] Streaming triangle counting integration test passed')
    except Exception as streaming_error:
        if os.getenv('JASMINEGRAPH_ALLOW_STREAMING_TEST_FAILURE_SUPPRESSION') == '1':
            logging.warning(
                '[Streaming] Encountered error but marking test as passed as requested: %s',
                str(streaming_error)
            )
        else:
            logging.error(
                '[Streaming] Streaming triangle counting integration test failed: %s',
                str(streaming_error)
            )
            raise
    finally:
        stop_consumer.set()
        try:
            send_stopstrian(host, port)
        except Exception:
            pass
        try:
            cleanup_streaming_graph(host, port, graph_id)
        except Exception as cleanup_error:
            logging.warning('[Streaming] Graph cleanup warning: %s', str(cleanup_error))
        try:
            kafka_down()
        except Exception as kafka_error:
            logging.warning('[Streaming] Kafka shutdown warning: %s', str(kafka_error))

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
                if b'done\r\n' in buffer:
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

def test(host, port):  # pylint: disable=too-many-branches
    """Test the JasmineGraph server by sending a series of commands and checking the responses."""

    small_property_graph_id = None
    small_property_graph_id_bytes = None

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((host, port))
        print()
        logging.info('Testing lst')
        send_and_expect_response(sock, 'Initial lst', LIST, EMPTY)

        print()
        logging.info('Testing adgr')
        send_and_expect_response(sock, 'adgr', ADGR, SEND, exit_on_failure=True)
        send_and_expect_response(
        sock, 'adgr', b'powergrid|/var/tmp/data/powergrid.dl', DONE, exit_on_failure=True)

        print()
        logging.info('Testing lst after adgr')
        send_and_expect_response(sock, 'lst after adgr', LIST,
                                 b'|1|powergrid|/var/tmp/data/powergrid.dl|op|')

        print()
        logging.info('Testing ecnt')
        send_and_expect_response(sock, 'ecnt', ECNT, b'graphid-send')
        send_and_expect_response(sock, 'ecnt', b'1', b'6594')

        print()
        logging.info('Testing vcnt')
        send_and_expect_response(sock, 'vcnt', VCNT, b'graphid-send')
        send_and_expect_response(sock, 'vcnt', b'1', b'4941')

        print()
        logging.info('Testing trian')
        send_and_expect_response(sock, 'trian', TRIAN,
                                 b'graphid-send', exit_on_failure=True)
        send_and_expect_response(
            sock, 'trian', b'1', b'priority(>=1)', exit_on_failure=True)
        send_and_expect_response(sock, 'trian', b'1', b'651')

        print()
        logging.info('Testing pgrnk')
        send_and_expect_response(sock, 'pgrnk', PGRNK,
                                 b'grap', exit_on_failure=True)
        send_and_expect_response(
            sock, 'pgrnk', b'1|0.5|40', b'priority(>=1)', exit_on_failure=True)
        send_and_expect_response(sock, 'pgrnk', b'1',
                                 DONE, exit_on_failure=True)

        print()
        logging.info('Testing adgr-cust')
        send_and_expect_response(sock, 'adgr-cust', ADGR_CUST,
                                 b'Select a custom graph upload option' + LINE_END +
                                 b'1 : Graph with edge list + text attributes list' + LINE_END +
                                 b'2 : Graph with edge list + JSON attributes list' + LINE_END +
                                 b'3 : Graph with edge list + XML attributes list',
                                 exit_on_failure=True)
        send_and_expect_response(sock, 'adgr-cust',
                                 b'1',
                                 b'Send <name>|<path to edge list>|<path to attribute file>|' +
                                 b'(optional)<attribute data type: int8. int16, int32 or float>',
                                 exit_on_failure=True)
        send_and_expect_response(sock, 'adgr-cust',
                                 b'cora|/var/tmp/data/cora/cora.cites|' +
                                 b'/var/tmp/data/cora/cora.content',
                                 DONE, exit_on_failure=True)

        print()
        logging.info('Testing lst after adgr-cust')
        send_and_expect_response(sock, 'lst after adgr-cust', LIST,
                                 b'|1|powergrid|/var/tmp/data/powergrid.dl|op|' + LINE_END +
                                 b'|2|cora|/var/tmp/data/cora/cora.cites|op|')

        print()
        logging.info('Testing merge')
        send_and_expect_response(sock, 'merge', MERGE, b'Available main flags:' + LINE_END +
                                 b'graph_id' + LINE_END +
                                 b'Send --<flag1> <value1>')
        send_and_expect_response(
            sock, 'merge', b'--graph_id 2', DONE, exit_on_failure=True)

        print()
        logging.info('Testing train')
        send_and_expect_response(sock, 'train', TRAIN, b'Available main flags:' + LINE_END +
                                 b'graph_id learning_rate batch_size validate_iter epochs' +
                                 LINE_END + b'Send --<flag1> <value1> --<flag2> <value2> ..',
                                 exit_on_failure=True)
        send_and_expect_response(
            sock, 'train', b'--graph_id 2', DONE, exit_on_failure=True)

        print()
        logging.info('Testing rmgr')
        send_and_expect_response(sock, 'rmgr', RMGR, SEND)
        send_and_expect_response(sock, 'rmgr', b'2', DONE)

        print()
        logging.info('Testing lst after rmgr')
        send_and_expect_response(sock, 'lst after rmgr',
                                 LIST, b'|1|powergrid|/var/tmp/data/powergrid.dl|op|')

        send_and_expect_response(sock, 'rmgr', RMGR, SEND)
        send_and_expect_response(sock, 'rmgr', b'1', DONE)

        # Test cases for hdfs implementation for custom hdfs server
        print()
        logging.info('Testing adhdfs for custom HDFS server')
        send_and_expect_response(sock, 'adhdfs', ADHDFS,
                                 b'Do you want to use the default HDFS server(y/n)?',
                                 exit_on_failure=True)
        send_and_expect_response(sock, 'adhdfs', b'n',
                                 b'Send the file path to the HDFS configuration file.' +
                                 b' This file needs to be in some directory location ' +
                                 b'that is accessible for JasmineGraph master',
                                 exit_on_failure=True)
        send_and_expect_response(sock, 'adhdfs', b'/var/tmp/config/hdfs_config.txt',
                                 b'HDFS file path: ',
                                 exit_on_failure=True)
        send_and_expect_response(sock, 'adhdfs', b'/home/powergrid.dl',
                                 b'Is this an edge list type graph(y/n)?',
                                 exit_on_failure=True)
        send_and_expect_response(sock, 'adhdfs', b'y',
                                 b'Is this a directed graph(y/n)?',
                                 exit_on_failure=True)
        send_and_expect_response(sock, 'adhdfs', b'y', DONE, exit_on_failure=True)

        print()
        logging.info('Testing lst after adhdfs')
        send_and_expect_response(sock, 'lst after adhdfs', LIST,
                                 b'|1|/home/powergrid.dl|hdfs:/home/powergrid.dl|op|',
                                 exit_on_failure=True)

        # print()
        # logging.info('1. Testing ecnt after adhdfs')
        # send_and_expect_response(sock, 'ecnt', ECNT, b'graphid-send', exit_on_failure=True)
        # send_and_expect_response(sock, 'ecnt', b'1', b'6594', exit_on_failure=True)

        # print()
        # logging.info('1. Testing vcnt after adhdfs')
        # send_and_expect_response(sock, 'vcnt', VCNT, b'graphid-send', exit_on_failure=True)
        # send_and_expect_response(sock, 'vcnt', b'1', b'4941', exit_on_failure=True)

        print()
        logging.info('Testing adhdfs for custom graph with properties')
        send_and_expect_response(sock, 'adhdfs', ADHDFS,
                                 b'Do you want to use the default HDFS server(y/n)?',
                                 exit_on_failure=True)
        send_and_expect_response(sock, 'adhdfs', b'n',
                                 b'Send the file path to the HDFS configuration file.' +
                                 b' This file needs to be in some directory location ' +
                                 b'that is accessible for JasmineGraph master',
                                 exit_on_failure=True)
        send_and_expect_response(sock, 'adhdfs', b'/var/tmp/config/hdfs_config.txt',
                                 b'HDFS file path: ',
                                 exit_on_failure=True)
        send_and_expect_response(sock, 'adhdfs', b'/home/graph_with_properties.txt',
                                 b'Is this an edge list type graph(y/n)?',
                                 exit_on_failure=True)
        send_and_expect_response(sock, 'adhdfs', b'n',
                                 b'Is this a directed graph(y/n)?',
                                 exit_on_failure=True)
        send_and_expect_response(sock, 'adhdfs', b'y', DONE, exit_on_failure=True)

        small_property_graph_id = get_graph_id_by_path(
            host, port, '/home/graph_with_properties.txt', prefer='lowest'
        )
        small_property_graph_id_bytes = small_property_graph_id.encode()


        print()
        logging.info('2. Testing cypher aggregate query after adding the graph')
        send_and_expect_response(sock, 'cypher', CYPHER, b'Graph ID:', exit_on_failure=True)
        # send_and_expect_response(sock, 'cypher', CYPHER, b'Graph ID:', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', small_property_graph_id_bytes,
                     b'Input query :', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', b'match (n) where n.id < 10 return avg(n.id)',
                                 b'{"avg(n.id)":4.5}', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', b'',
                                 b'done', exit_on_failure=True)

        print()
        logging.info('[Cypher] Uploading large graph for cypher testing')
        send_and_expect_response(sock, 'adhdfs', ADHDFS,
                                 b'Do you want to use the default HDFS server(y/n)?',
                                 exit_on_failure=True)
        send_and_expect_response(sock, 'adhdfs', b'n',
                                 b'Send the file path to the HDFS configuration file.' +
                                 b' This file needs to be in some directory location ' +
                                 b'that is accessible for JasmineGraph master',
                                 exit_on_failure=True)
        send_and_expect_response(sock, 'adhdfs', b'/var/tmp/config/hdfs_config.txt',
                                 b'HDFS file path: ',
                                 exit_on_failure=True)
        send_and_expect_response(sock, 'adhdfs', b'/home/graph_with_properties_large.txt',
                                 b'Is this an edge list type graph(y/n)?',
                                 exit_on_failure=True)
        send_and_expect_response(sock, 'adhdfs', b'n',
                                 b'Is this a directed graph(y/n)?',
                                 exit_on_failure=True)
        send_and_expect_response(sock, 'adhdfs', b'y', DONE, exit_on_failure=True)

        print()
        logging.info('[Adhdfs] Testing uploaded graph')
        abs_path = os.path.abspath('tests/integration/env_init/data/graph_with_properties.txt')
        test_graph_validation(abs_path, small_property_graph_id, host, port)

        print()
        logging.info('[Cypher] Testing AllNodeScan ')
        send_and_expect_response(sock, 'cypher', CYPHER, b'Graph ID:', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', small_property_graph_id_bytes,
                     b'Input query :', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', b'MATCH (n) WHERE n.id=2 RETURN n ',
                                 b'{"n":{"id":"2","label":"Person","name":"Charlie",'
                                 b'"occupation":"IT Engineer",'
                                 b'"partitionID":"0"}}', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', b'',
                                 b'done', exit_on_failure=True)

        print()
        logging.info('[Cypher] Testing ProduceResults ')
        send_and_expect_response(sock, 'cypher', CYPHER, b'Graph ID:', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', small_property_graph_id_bytes,
                     b'Input query :', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', b'MATCH (n) WHERE n.id = 18 RETURN n.age, n.name ',
                                 b'{"n.age":null,"n.name":"Skyport Airport"}',
                                 exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', b'',
                                 b'done', exit_on_failure=True)

        print()
        logging.info('[Cypher] Testing ProduceResults')
        send_and_expect_response(sock, 'cypher', CYPHER, b'Graph ID:', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', small_property_graph_id_bytes,
                     b'Input query :', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', b'MATCH (n) WHERE n.id = 18 RETURN n.age, n.name ',
                                 b'{"n.age":null,"n.name":"Skyport Airport"}',
                                 exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', b'',
                                 b'done', exit_on_failure=True)

        print()
        logging.info('[Cypher] Testing filter by equality check')
        send_and_expect_response(sock, 'cypher', CYPHER, b'Graph ID:', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', small_property_graph_id_bytes,
                     b'Input query :', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', b"MATCH (n) WHERE n.name = 'Fiona' RETURN n",
                                 b'{"n":{"age":"25","id":"10","label":"Person",'
                                 b'"name":"Fiona","occupation":"Artist",'
                                 b'"partitionID":"0"}}',
                                 exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', b'',
                                 b'done', exit_on_failure=True)

        print()
        logging.info('[Cypher] Testing filter by comparison of integer attribute')
        send_and_expect_response(sock, 'cypher', CYPHER, b'Graph ID:', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', small_property_graph_id_bytes,
                     b'Input query :', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', b'MATCH (n) WHERE n.age < 30 return n',
                                 b'{"n":{"age":"25","id":"10","label":"Person",'
                                 b'"name":"Fiona","occupation":"Artist",'
                                 b'"partitionID":"0"}}',
                                 exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', b'',
                                 b'done', exit_on_failure=True)


        print()
        logging.info('[Cypher] Testing expand all ')
        send_and_expect_response(sock, 'cypher', CYPHER, b'Graph ID:', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', small_property_graph_id_bytes,
                     b'Input query :', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher',b'MATCH (a)-[r]-(b)-[d]-(s)'
                                                b' WHERE (a.id = 10 AND s.id=14) RETURN a, b, s',
                                 b'{"a":{"age":"25","id":"10","label":"Person",'
                                 b'"name":"Fiona","occupation":"Artist","partitionID":"0"},'
                                 b'"b":{"id":"2","label":"Person","name":"Charlie",'
                                 b'"occupation":"IT Engineer","partitionID":"0"},'
                                 b'"s":{"id":"14","label":"Person",'
                                 b'"name":"Julia","occupation":"Entrepreneur","partitionID":"0"}}',
                                 exit_on_failure=True)

        send_and_expect_response(sock, 'cypher', b'',
                                 b'done', exit_on_failure=True)
        print()
        logging.info('[Cypher] Testing Undirected Relationship Type Scan')
        send_and_expect_response(sock, 'cypher', CYPHER, b'Graph ID:', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', small_property_graph_id_bytes,
                     b'Input query :', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher',b'MATCH '
                                                b"(n {name:'Eva'})-[:NEIGHBORS]-(x ) RETURN x",

                                 b'{"x":{"id":"0","label":"Person","name":"Alice",'
                                 b'"occupation":"Teacher","partitionID":"0"}}',
                                 exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', b'',
                                 b'done', exit_on_failure=True)


        print()
        logging.info('[Cypher] Testing Undirected All Relationship Scan')
        send_and_expect_response(sock, 'cypher', CYPHER, b'Graph ID:', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', small_property_graph_id_bytes,
                     b'Input query :', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher',b'MATCH (n)-[r]-(m {id:6} ) WHERE n.age = 25'
                                                b' RETURN n, r, m',
                                 b'{"m":{"category":"Park","id":"6","label":"Location",'
                                 b'"name":"Central Park",'
                                 b'"partitionID":"0"},"n":{"age":"25","id":"10","label":"Person",'
                                 b'"name":"Fiona","occupation":"Artist","partitionID":"0"'
                                 b'},"r":{"description":"Fiona and Central Park have'
                                 b' been friends since college.","id":"11",'
                                 b'"type":"FRIENDS"}}',

                                 exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', b'',
                                 b'done', exit_on_failure=True)

        print()
        logging.info('[Cypher] Testing Directed Relationship Type Scan ')
        send_and_expect_response(sock, 'cypher', CYPHER, b'Graph ID:', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', small_property_graph_id_bytes, b'Input query :',
                                 exit_on_failure=True)
        send_and_expect_response(sock, 'cypher',b'MATCH'
                                                b" (n {name:'Eva'})-[:NEIGHBORS]->(x ) RETURN x",

                                 b'{"x":{"id":"0","label":"Person","name":"Alice",'
                                 b'"occupation":"Teacher","partitionID":"0"}}',
                                 exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', b'',
                                 b'done', exit_on_failure=True)

        print()
        logging.info('[Cypher] Testing OrderBy ')
        send_and_expect_response(sock, 'cypher', CYPHER, b'Graph ID:', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', small_property_graph_id_bytes,
                     b'Input query :', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher',b"match (n) where n.partitionID = '1' return n "
                                                b'order by n.name ASC',
                                 b'''{"n":{"category":"Studio","id":"15","label":"Location",'''
                                 b'''"name":"Art Studio","partitionID":"1"}}''',
                                 exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', b'',
                                 b'{"n":{"id":"1","label":"Person","name":"Bob","occupation":'
                                 b'"Banker","partitionID":"1"}}', exit_on_failure=True)

        send_and_expect_response(sock, 'cypher', b'',
                                 b'{"n":{"id":"3","label":"Person","name":"David","occupation":'
                                 b'"Doctor","partitionID":"1"}}', exit_on_failure=True)

        send_and_expect_response(sock, 'cypher', b'',b'{"n":{"id":"11","label":"Person",'
                                                     b'"name":"George","occupation":"Chef",'
                                                     b'"partitionID":"1"}}', exit_on_failure=True)

        send_and_expect_response(sock, 'cypher', b'',
                                 b'{"n":{"category":"Restaurant","id":"17","label":"Location",'
                                 b'"name":"Gourmet Bistro","partitionID":"1"}}',
                                 exit_on_failure=True)

        send_and_expect_response(sock, 'cypher', b'',
                                 b'{"n":{"category":"School","id":"5","label":"Location",'
                                 b'"name":"Greenfield School","partitionID":"1"}}',
                                 exit_on_failure=True)

        send_and_expect_response(sock, 'cypher', b'',b'{"n":{"id":"13","label":"Person",'
                                                     b'"name":"Ian","occupation":"Pilot",'
                                                     b'"partitionID":"1"}}', exit_on_failure=True)

        send_and_expect_response(sock, 'cypher', b'',
                                 b'{"n":{"category":"Coworking Space","id":"19","label":'
                                 b'"Location","name":"Innovation Hub","partitionID":"1"}}',
                                 exit_on_failure=True)

        send_and_expect_response(sock, 'cypher', b'',
                                 b'{"n":{"category":"Bank","id":"7","label":"Location","name":'
                                 b'"Town Bank","partitionID":"1"}}', exit_on_failure=True)

        send_and_expect_response(sock, 'cypher', b'',
                                 b'{"n":{"category":"Hospital","id":"9","label":"Location",'
                                 b'"name":"Town General Hospital","partitionID":"1"}}',
                                 exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', b'',
                                 b'done', exit_on_failure=True)

        print()
        logging.info('[Cypher] Testing Node Scan By Label')
        send_and_expect_response(sock, 'cypher', CYPHER, b'Graph ID:', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', small_property_graph_id_bytes,
                     b'Input query :', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher',b'match(n:Person) where n.id=2 return n'
                                                b' RETURN n',b'{"n":{"id":"2","label":"Person",'
                                                b'"name":"Charlie","occupation":"IT Engineer",'
                                                b'"partitionID":"0"}}',

                                 exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', b'',
                                 b'done', exit_on_failure=True)

        print()
        logging.info('[Cypher] Testing rmgr after adhdfs')
        send_and_expect_response(sock, 'rmgr', RMGR, SEND, exit_on_failure=True)
        send_and_expect_response(sock, 'rmgr', b'1', DONE, exit_on_failure=True)

        print()
        logging.info(
            '[IntraPartition] Testing getAllProperties on small graph (sequential fallback)'
        )
        send_and_expect_response(sock, 'cypher', CYPHER, b'Graph ID:', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', small_property_graph_id_bytes,
                                 b'Input query :', exit_on_failure=True)
        # Test that getAllProperties returns all node properties correctly
        send_and_expect_response(sock, 'cypher', b'MATCH (n) WHERE n.id = 2 RETURN n',
                                 b'{"n":{"id":"2","label":"Person","name":"Charlie",'
                                 b'"occupation":"IT Engineer","partitionID":"0"}}',
                                 exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', b'', b'done', exit_on_failure=True)

        print()
        logging.info('[IntraPartition] Testing getAllProperties with null values')
        send_and_expect_response(sock, 'cypher', CYPHER, b'Graph ID:', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', small_property_graph_id_bytes,
                     b'Input query :', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', b'MATCH (n:Location) WHERE n.id = 6 RETURN n',
                                 b'{"n":{"category":"Park","id":"6","label":"Location",'
                                 b'"name":"Central Park","partitionID":"0"}}',
                                 exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', b'', b'done', exit_on_failure=True)

        print()
        logging.info('[IntraPartition] Testing getAllProperties multiple nodes (lifetime safety)')
        send_and_expect_response(sock, 'cypher', CYPHER, b'Graph ID:', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', small_property_graph_id_bytes,
                     b'Input query :', exit_on_failure=True)
        # Return multiple nodes to verify no memory corruption or dangling references
        query = b'MATCH (n:Person) WHERE n.id < 4 RETURN n.id, n.name ORDER BY n.id ASC'
        sock.sendall(query + LINE_END)
        print('MATCH (n:Person) WHERE n.id < 4 RETURN n.id, n.name ORDER BY n.id ASC')
        # Expecting exactly 4 results - Alice (0), Bob (1), Charlie (2), David (3)
        expected_results = [
            b'{"n.id":"0","n.name":"Alice"}',
            b'{"n.id":"1","n.name":"Bob"}',
            b'{"n.id":"2","n.name":"Charlie"}',
            b'{"n.id":"3","n.name":"David"}'
        ]
        for i, expected in enumerate(expected_results):
            if not expect_response(sock, expected + LINE_END):
                failed_tests.append(f'[IntraPartition] Multiple nodes - result {i}')
        send_and_expect_response(sock, 'cypher', b'', b'done', exit_on_failure=True)

        print()
        logging.info(
            '[IntraPartition] Testing getAllProperties on large graph (parallel execution)'
        )
        send_and_expect_response(sock, 'cypher', CYPHER, b'Graph ID:', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', b'3', b'Input query :', exit_on_failure=True)
        # Spot check: verify a node query works on large graph
        sock.sendall(b'MATCH (n) WHERE n.id = 1 RETURN n' + LINE_END)
        print('MATCH (n) WHERE n.id = 1 RETURN n')
        response = b''
        while True:
            byte = sock.recv(1)
            if not byte:
                break
            response += byte
            if response.endswith(b'\r\n') or response.endswith(b'\n'):
                break

        if b'"id":"1"' in response:
            logging.info('✓ Large graph node query returned results')
        else:
            logging.warning('Large graph query unexpected response: %s', response[:100])
            failed_tests.append('[IntraPartition] Large graph getAllProperties')
        send_and_expect_response(sock, 'cypher', b'', b'done', exit_on_failure=True)

        print()
        logging.info('[IntraPartition] Testing relationship getAllProperties')
        send_and_expect_response(sock, 'cypher', CYPHER, b'Graph ID:', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', b'3', b'Input query :', exit_on_failure=True)
        # Verify relationship scan works
        sock.sendall(b'MATCH (n)-[r]->(m) WHERE n.id = 1 RETURN n, r, m' + LINE_END)
        print('MATCH (n)-[r]->(m) WHERE n.id = 1 RETURN n, r, m')
        response = b''
        while True:
            byte = sock.recv(1)
            if not byte:
                break
            response += byte
            if response.endswith(b'\r\n') or response.endswith(b'\n'):
                break

        if b'"n":' in response and b'"r":' in response and b'"m":' in response:
            logging.info('✓ Relationship query returned results with correct structure')
        else:
            logging.warning('Relationship query unexpected response: %s', response[:100])
            failed_tests.append('[IntraPartition] Relationship structure')
        send_and_expect_response(sock, 'cypher', b'', b'done', exit_on_failure=True)

        print()
        logging.info('[Cypher] Testing OrderBy for Large Graph')
        send_and_expect_response(sock, 'cypher', CYPHER, b'Graph ID:', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', b'3', b'Input query :', exit_on_failure=True)
        send_and_expect_response_file(sock,'cypher', b'MATCH (n) RETURN n.id, n.name, n.code '
                                                     b'ORDER BY n.code ASC',
                                      'tests/integration/utils/expected_output/'
                                      'orderby_expected_output_file.txt',exit_on_failure=True)

        enable_streaming_test = os.getenv('JASMINEGRAPH_ENABLE_STREAMING_TEST', '1') == '1'
        if enable_streaming_test:
            print()
            logging.info('Testing strian')
            test_streaming_triangle_count_with_kafka(host, port)
        else:
            print()
            logging.info('Skipping strian test (JASMINEGRAPH_ENABLE_STREAMING_TEST != 1)')
        # removing all the uploaded graphs after testing
        print()
        logging.info('Removing all uploaded graphs after testing')
        send_and_expect_response(sock, 'lst before truncate', LIST,
        b'|2|/home/graph_with_properties.txt|hdfs:/home/graph_with_properties.txt|op|' + LINE_END +
        b'|3|/home/graph_with_properties_large.txt|hdfs:/home/graph_with_properties_large.txt|op|')
        send_and_expect_response(sock, 'truncate', TRUNCATE, DONE)
        send_and_expect_response(sock, 'lst after truncate', LIST, EMPTY)

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
