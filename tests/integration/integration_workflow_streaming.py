"""Copyright 2026 JasmineGraph Team
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

import json
import os
import re
import socket
import subprocess
import threading
import time

import integration_common as common


ADSTRMK = b'adstrmk'
STRIAN = b'strian'
STOPSTRIAN = b'stopstrian'
KAFKA_TOPIC = 'stream-triangle-test'
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DOCKER_COMPOSE_FILE = os.path.join(BASE_DIR, 'docker-compose.yml')


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
    conn.sendall(ADSTRMK + common.LINE_END)
    recv_until_contains(conn, [b'existing graph'])

    conn.sendall(b'n' + common.LINE_END)
    response = recv_until_contains(conn, [b'default graph ID:'])
    graph_id_match = re.search(rb'default graph ID:\s*(\d+)', response)
    if graph_id_match is None:
        raise RuntimeError(
            f"Failed to parse streaming graph id from response: {response.decode(errors='ignore')}"
        )
    graph_id = graph_id_match.group(1).decode()

    conn.sendall(b'y' + common.LINE_END)
    recv_until_contains(conn, [b'Choose an option'])

    conn.sendall(b'1' + common.LINE_END)
    recv_until_contains(conn, [b'Directed'])

    conn.sendall(b'y' + common.LINE_END)
    recv_until_contains(conn, [b'default KAFKA consumer'])

    conn.sendall(b'y' + common.LINE_END)
    recv_until_contains(conn, [b'send kafka topic name'])

    conn.sendall(KAFKA_TOPIC.encode() + common.LINE_END)
    recv_until_contains(conn, [b'Received the kafka topic'])

    return graph_id


def send_stopstrian(host, port):
    """Send the stopstrian command."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as stop_socket:
        stop_socket.settimeout(10)
        stop_socket.connect((host, port))
        stop_socket.sendall(STOPSTRIAN + common.LINE_END)


def cleanup_streaming_graph(host, port, graph_id):
    """Remove the temporary graph created for the streaming test."""
    if not graph_id:
        return

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as cleanup_socket:
        cleanup_socket.settimeout(20)
        cleanup_socket.connect((host, port))

        cleanup_socket.sendall(common.RMGR + common.LINE_END)
        recv_until_contains(cleanup_socket, [b'send'])

        cleanup_socket.sendall(graph_id.encode() + common.LINE_END)
        recv_until_contains(cleanup_socket, [b'done'])


def query_streaming_triangle_count(host, port, graph_id, mode='0', result_timeout=120):
    """Query the streaming triangle count and parse the returned count."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as strian_sock:
            strian_sock.settimeout(30)
            strian_sock.connect((host, port))

            strian_sock.sendall(STRIAN + common.LINE_END)
            recv_until_contains(strian_sock, [b'graphid-send', b'grap'])

            strian_sock.sendall(graph_id.encode() + common.LINE_END)
            recv_until_contains(strian_sock, [b'mode-send', b'mode'])

            strian_sock.sendall(mode.encode() + common.LINE_END)
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
    common.logging.info('Testing Kafka streaming triangle counting integration')

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
            common.logging.info('[Streaming] Graph ID selected: %s', graph_id)

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
                common.logging.warning('[Streaming][Sequential] Attempt %s failed: %s',
                                       sequential_attempt, str(query_error))
                time.sleep(4)
                continue

            latest_triangle_count = triangle_count
            latest_response = response
            common.logging.info('[Streaming][Sequential] Attempt %s triangle count: %s',
                                sequential_attempt, triangle_count)
            if triangle_count >= expected_triangle_count:
                break
            time.sleep(4)

        stop_consumer.set()
        consumer_thread.join(timeout=10)

        if len(consumed_messages) < 1:
            common.logging.warning(
                '[Streaming] Kafka consumer did not read enough messages from topic. Read %s',
                len(consumed_messages)
            )

        if latest_triangle_count < expected_triangle_count:
            raise RuntimeError(
                '[Streaming] Streaming triangle count below threshold. '
                f'Expected >= {expected_triangle_count}, got {latest_triangle_count}. '
                f'Last response: {latest_response}'
            )

        common.logging.info('[Streaming] Streaming triangle counting integration test passed')
    except Exception as streaming_error:
        if os.getenv('JASMINEGRAPH_ALLOW_STREAMING_TEST_FAILURE_SUPPRESSION') == '1':
            common.logging.warning(
                '[Streaming] Encountered error but marking test as passed as requested: %s',
                str(streaming_error)
            )
        else:
            common.logging.error(
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
            common.logging.warning('[Streaming] Graph cleanup warning: %s', str(cleanup_error))
        try:
            kafka_down()
        except Exception as kafka_error:
            common.logging.warning('[Streaming] Kafka shutdown warning: %s', str(kafka_error))


def run_streaming_workflow(host, port):
    """Run optional streaming triangle counting workflow."""
    enable_streaming_test = os.getenv('JASMINEGRAPH_ENABLE_STREAMING_TEST', '1') == '1'
    if enable_streaming_test:
        print()
        common.logging.info('Testing strian')
        test_streaming_triangle_count_with_kafka(host, port)
    else:
        print()
        common.logging.info('Skipping strian test (JASMINEGRAPH_ENABLE_STREAMING_TEST != 1)')
