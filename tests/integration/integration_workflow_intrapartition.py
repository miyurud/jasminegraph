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

import integration_common as common


GRAPH_ID_PROMPT = b'Graph ID:'
INPUT_QUERY_PROMPT = b'Input query :'


def _start_graph_query(sock, graph_id):
    """Open query prompt for the given graph id."""
    common.send_and_expect_response(
        sock,
        'cypher',
        common.CYPHER,
        GRAPH_ID_PROMPT,
        exit_on_failure=True,
    )
    common.send_and_expect_response(
        sock,
        'cypher',
        graph_id,
        INPUT_QUERY_PROMPT,
        exit_on_failure=True,
    )


def _read_query_line(sock):
    """Read one response line from socket for query validation."""
    response = b''
    while True:
        byte = sock.recv(1)
        if not byte:
            break
        response += byte
        if response.endswith(b'\r\n') or response.endswith(b'\n'):
            break
    return response


def run_intrapartition_workflow(sock):
    """Run getAllProperties and large-graph cypher checks."""
    print()
    common.logging.info(
        '[IntraPartition] Testing getAllProperties on small graph (sequential fallback)'
    )
    _start_graph_query(sock, b'2')
    common.send_and_expect_response(
        sock,
        'cypher',
        b'MATCH (n) WHERE n.id = 2 RETURN n',
        b'{"n":{"id":"2","label":"Person","name":"Charlie",'
        b'"occupation":"IT Engineer","partitionID":"0"}}',
        exit_on_failure=True,
    )
    common.send_and_expect_response(sock, 'cypher', b'', b'done', exit_on_failure=True)

    print()
    common.logging.info('[IntraPartition] Testing getAllProperties with null values')
    _start_graph_query(sock, b'2')
    common.send_and_expect_response(
        sock,
        'cypher',
        b'MATCH (n:Location) WHERE n.id = 6 RETURN n',
        b'{"n":{"category":"Park","id":"6","label":"Location",'
        b'"name":"Central Park","partitionID":"0"}}',
        exit_on_failure=True,
    )
    common.send_and_expect_response(sock, 'cypher', b'', b'done', exit_on_failure=True)

    print()
    common.logging.info(
        '[IntraPartition] Testing getAllProperties multiple nodes (lifetime safety)'
    )
    _start_graph_query(sock, b'2')
    query = b'MATCH (n:Person) WHERE n.id < 4 RETURN n.id, n.name ORDER BY n.id ASC'
    sock.sendall(query + common.LINE_END)
    print('MATCH (n:Person) WHERE n.id < 4 RETURN n.id, n.name ORDER BY n.id ASC')
    expected_results = [
        b'{"n.id":"0","n.name":"Alice"}',
        b'{"n.id":"1","n.name":"Bob"}',
        b'{"n.id":"2","n.name":"Charlie"}',
        b'{"n.id":"3","n.name":"David"}'
    ]
    for i, expected in enumerate(expected_results):
        if not common.expect_response(sock, expected + common.LINE_END):
            common.failed_tests.append(f'[IntraPartition] Multiple nodes - result {i}')
    common.send_and_expect_response(sock, 'cypher', b'', b'done', exit_on_failure=True)

    print()
    common.logging.info(
        '[IntraPartition] Testing getAllProperties on large graph (parallel execution)'
    )
    _start_graph_query(sock, b'4')
    sock.sendall(b'MATCH (n) WHERE n.id = 1 RETURN n' + common.LINE_END)
    print('MATCH (n) WHERE n.id = 1 RETURN n')
    response = _read_query_line(sock)

    if b'"id":"1"' in response:
        common.logging.info('✓ Large graph node query returned results')
    else:
        common.logging.warning('Large graph query unexpected response: %s', response[:100])
        common.failed_tests.append('[IntraPartition] Large graph getAllProperties')
    common.send_and_expect_response(sock, 'cypher', b'', b'done', exit_on_failure=True)

    print()
    common.logging.info('[IntraPartition] Testing relationship getAllProperties')
    _start_graph_query(sock, b'4')
    sock.sendall(b'MATCH (n)-[r]->(m) WHERE n.id = 1 RETURN n, r, m' + common.LINE_END)
    print('MATCH (n)-[r]->(m) WHERE n.id = 1 RETURN n, r, m')
    response = _read_query_line(sock)

    if b'"n":' in response and b'"r":' in response and b'"m":' in response:
        common.logging.info('✓ Relationship query returned results with correct structure')
    else:
        common.logging.warning('Relationship query unexpected response: %s', response[:100])
        common.failed_tests.append('[IntraPartition] Relationship structure')
    common.send_and_expect_response(sock, 'cypher', b'', b'done', exit_on_failure=True)

    print()
    common.logging.info('[Cypher] Testing OrderBy for Large Graph')
    _start_graph_query(sock, b'4')
    common.send_and_expect_response_file(
        sock,
        'cypher',
        b'MATCH (n) RETURN n.id, n.name, n.code '
        b'ORDER BY n.code ASC',
        'tests/integration/utils/expected_output/'
        'orderby_expected_output_file.txt',
        exit_on_failure=True,
    )
