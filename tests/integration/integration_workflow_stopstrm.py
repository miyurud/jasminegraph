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
import socket
import subprocess
import time

import integration_common as common

KAFKA_BROKER = '172.28.5.8:9092'
KAFKA_CONTAINER = 'jasminegraph-kafka-1'
TOPIC_1 = 'stopstrm-topic-1'
TOPIC_2 = 'stopstrm-topic-2'


def _create_topic(topic_name):
    """Create a Kafka topic if it does not already exist."""
    command = [
        'docker', 'exec', KAFKA_CONTAINER,
        '/opt/kafka/bin/kafka-topics.sh',
        '--bootstrap-server', KAFKA_BROKER,
        '--create', '--if-not-exists',
        '--topic', topic_name,
        '--partitions', '1',
        '--replication-factor', '1',
    ]
    subprocess.run(command, check=True, capture_output=True, text=True)


def _recv_until(sock, needles, timeout=10):
    if isinstance(needles, str):
        needles = [needles]

    deadline = time.time() + timeout
    data = bytearray()

    while time.time() < deadline:
        try:
            chunk = sock.recv(4096)
        except socket.timeout:
            continue

        if not chunk:
            break

        data.extend(chunk)
        text = data.decode(errors='replace')
        if any(needle in text for needle in needles):
            return text

    return data.decode(errors='replace')


def _send_line(sock, text):
    if isinstance(text, bytes):
        payload = text + b'\r\n'
    else:
        payload = (text + '\r\n').encode()
    sock.sendall(payload)


def _start_stream(sock, graph_id, topic_name):
    """Start a Kafka stream using the existing add-stream flow."""
    _recv_until(sock, 'Do you want to stream into existing graph')
    _send_line(sock, 'adstrmk')

    _recv_until(sock, 'Do you want to stream into existing graph')
    _send_line(sock, 'n')

    _recv_until(sock, 'Do you use default graph ID')
    _send_line(sock, 'n')

    _recv_until(sock, 'Input your graph ID:')
    _send_line(sock, graph_id)

    _recv_until(sock, 'Choose an option(1,2,3):')
    _send_line(sock, '1')

    _recv_until(sock, 'Is this graph Directed (y/n)?')
    _send_line(sock, 'n')

    _recv_until(sock, 'Do you want to use default KAFKA consumer(y/n) ?')
    _send_line(sock, 'y')

    _recv_until(sock, 'send kafka topic name')
    _send_line(sock, topic_name)

    _recv_until(sock, 'Received the kafka topic')


def _stop_stream_by_topic(sock, topic_name):
    """Stop a stream using stopstrm with inline topic name."""
    _send_line(sock, f'stopstrm {topic_name}')
    response = _recv_until(
        sock,
        [
            f'Successfully initiated stop for topic `{topic_name}`',
            f'Error: No active stream found for topic `{topic_name}`',
            'Error:',
        ],
    )
    assert f'Successfully initiated stop for topic `{topic_name}`' in response


def run_stopstrm_workflow(host, port):
    """Run a multi-stream stop workflow using Kafka topics."""
    print()
    common.logging.info('Testing stopstrm with Kafka topics')
    _create_topic(TOPIC_1)
    _create_topic(TOPIC_2)

    first_graph_id = b'9101'
    second_graph_id = b'9102'

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock1, socket.socket(
        socket.AF_INET, socket.SOCK_STREAM
    ) as sock2:
        sock1.connect((host, port))
        sock2.connect((host, port))

        _start_stream(sock1, first_graph_id, TOPIC_1)
        _start_stream(sock2, second_graph_id, TOPIC_2)

        _stop_stream_by_topic(sock1, TOPIC_1)

        _send_line(sock1, f'stopstrm {TOPIC_1}')
        response = _recv_until(sock1, [f'Error: No active stream found for topic `{TOPIC_1}`', 'Error:'])
        assert f'Error: No active stream found for topic `{TOPIC_1}`' in response

        _stop_stream_by_topic(sock1, TOPIC_2)

        print()
        common.logging.info('Testing stopstrm by inline topic completed')
