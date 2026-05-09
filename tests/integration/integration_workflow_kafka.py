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
import time

import integration_common as common


def _start_kafka_stream(sock, graph_id, topic_name):
    """Start a Kafka stream for an existing graph id and topic name."""
    topic_bytes = topic_name.encode('utf-8')

    common.send_and_expect_response_contains(
        sock,
        'adstrmk existing graph prompt',
        common.ADSTRMK,
        b'Do you want to stream into existing graph(y/n) ? ',
        exit_on_failure=True,
    )
    common.send_and_expect_response_contains(
        sock,
        'adstrmk graph id prompt',
        b'y',
        b'Send the existing graph ID ? ',
        exit_on_failure=True,
    )
    common.send_and_expect_response(
        sock,
        'adstrmk graph id',
        str(graph_id).encode('utf-8'),
        f'Set data streaming into graph ID: {graph_id}'.encode('utf-8'),
        exit_on_failure=True,
    )
    common.expect_response_contains_and_record(
        sock,
        'adstrmk default kafka prompt',
        b'Do you want to use default KAFKA consumer(y/n) ?',
        exit_on_failure=True,
    )
    common.send_and_expect_response(
        sock,
        'adstrmk default kafka',
        b'y',
        b'send kafka topic name',
        exit_on_failure=True,
    )
    common.send_and_expect_response(
        sock,
        'adstrmk topic',
        topic_bytes,
        b'Received the kafka topic',
        exit_on_failure=True,
    )

    return topic_bytes


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
    _start_kafka_stream(sock, 1, topic_name)
    _stop_kafka_stream(sock, topic_name, 1)

    print()
    common.logging.info('[Kafka] Testing stopstrm prompt for graph id')

    shared_topic = f'jg_it_kafka_shared_{int(time.time())}'
    _start_kafka_stream(sock, 1, shared_topic)
    _start_kafka_stream(sock, 2, shared_topic)

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
        b'1',
        (
            f'Successfully initiated stop for topic `{shared_topic}` (graph ID 1)'
        ).encode('utf-8'),
        exit_on_failure=True,
    )

    _stop_kafka_stream(sock, shared_topic, 2)
