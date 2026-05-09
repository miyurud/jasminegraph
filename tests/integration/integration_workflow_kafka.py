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
import re
import sys
import time

import integration_common as common


DEFAULT_GRAPH_ID_PATTERN = re.compile(rb'Do you use default graph ID: ([0-9]+)\(y/n\) \?')


def _fail(test_name):
    """Record a Kafka workflow failure and stop the integration run."""
    common.failed_tests.append(test_name)
    print()
    common.logging.fatal('Failed some tests,')
    print(*common.failed_tests, sep='\n', file=sys.stderr)
    sys.exit(1)


def _send(sock, value):
    """Send one line to the frontend and echo it like other integration helpers."""
    sock.sendall(value + common.LINE_END)
    print(value.decode('utf-8'))


def _send_and_capture_response_contains(sock, test_name, send, expected):
    """Send a command and return the response buffer containing expected bytes."""
    _send(sock, send)
    response = common.receive_response_contains(sock, expected)
    if response is None:
        _fail(test_name)
    return response


def _start_kafka_stream(sock, topic_name):
    """Start a Kafka stream for a new graph and return the graph id."""
    topic_bytes = topic_name.encode('utf-8')

    common.send_and_expect_response_contains(
        sock,
        'adstrmk existing graph prompt',
        common.ADSTRMK,
        b'Do you want to stream into existing graph(y/n) ? ',
        exit_on_failure=True,
    )
    default_prompt = _send_and_capture_response_contains(
        sock,
        'adstrmk default graph id prompt',
        b'n',
        b'Do you use default graph ID: ',
    )

    match = DEFAULT_GRAPH_ID_PATTERN.search(default_prompt)
    if not match:
        common.logging.warning(
            'Could not parse default graph ID from response: %s',
            default_prompt.decode('utf-8', errors='replace'),
        )
        _fail('adstrmk default graph id prompt')

    graph_id = int(match.group(1))

    common.send_and_expect_response_contains(
        sock,
        'adstrmk accept default graph id',
        b'y',
        b'Choose an option(1,2,3): ',
        exit_on_failure=True,
    )
    common.send_and_expect_response_contains(
        sock,
        'adstrmk partition option',
        b'1',
        b'Is this graph Directed (y/n)? ',
        exit_on_failure=True,
    )
    common.send_and_expect_response_contains(
        sock,
        'adstrmk graph direction',
        b'y',
        b'Do you want to use default KAFKA consumer(y/n) ?',
        exit_on_failure=True,
    )
    common.send_and_expect_response_contains(
        sock,
        'adstrmk default kafka',
        b'y',
        b'send kafka topic name',
        exit_on_failure=True,
    )
    common.send_and_expect_response_contains(
        sock,
        'adstrmk topic',
        topic_bytes,
        b'Received the kafka topic',
        exit_on_failure=True,
    )

    return graph_id


def _stop_kafka_stream(sock, topic_name, graph_id):
    """Stop a Kafka stream by topic name and expected graph id."""
    topic_bytes = topic_name.encode('utf-8')
    stop_command = common.STOPSTRM + b' ' + topic_bytes
    expected_stop = (
        f'Successfully initiated stop for topic `{topic_name}` (graph ID {graph_id})'
    ).encode('utf-8')
    common.send_and_expect_response(
        sock,
        'stopstrm',
        stop_command,
        expected_stop,
        exit_on_failure=True,
    )


def run_kafka_workflow(sock):
    """Run Kafka stream stop integration test sequence."""
    print()
    common.logging.info('[Kafka] Testing adstrmk + stopstrm by topic')

    topic_name = f'jg_it_kafka_stop_{int(time.time())}'
    graph_id = _start_kafka_stream(sock, topic_name)
    _stop_kafka_stream(sock, topic_name, graph_id)

    print()
    common.logging.info('[Kafka] Testing stopstrm prompt for graph id')

    shared_topic = f'jg_it_kafka_shared_{int(time.time())}'
    first_graph_id = _start_kafka_stream(sock, shared_topic)
    second_graph_id = _start_kafka_stream(sock, shared_topic)

    stop_command = common.STOPSTRM + b' ' + shared_topic.encode('utf-8')
    common.send_and_expect_response_contains(
        sock,
        'stopstrm multi-stream prompt',
        stop_command,
        f'Multiple active streams found for topic `{shared_topic}`.'.encode('utf-8'),
        exit_on_failure=True,
    )
    common.send_and_expect_response(
        sock,
        'stopstrm disambiguate graph id',
        str(first_graph_id).encode('utf-8'),
        (
            f'Successfully initiated stop for topic `{shared_topic}` (graph ID {first_graph_id})'
        ).encode('utf-8'),
        exit_on_failure=True,
    )

    _stop_kafka_stream(sock, shared_topic, second_graph_id)
