"""Intra-partition integration workflow."""

import integration_common as common


def run_intrapartition_workflow(sock):
    """Run getAllProperties and large-graph cypher checks."""
    print()
    common.logging.info(
        '[IntraPartition] Testing getAllProperties on small graph (sequential fallback)'
    )
    common.send_and_expect_response(sock, 'cypher', common.CYPHER, b'Graph ID:', exit_on_failure=True)
    common.send_and_expect_response(sock, 'cypher', b'2', b'Input query :', exit_on_failure=True)
    common.send_and_expect_response(sock, 'cypher', b'MATCH (n) WHERE n.id = 2 RETURN n',
                                    b'{"n":{"id":"2","label":"Person","name":"Charlie",'
                                    b'"occupation":"IT Engineer","partitionID":"0"}}',
                                    exit_on_failure=True)
    common.send_and_expect_response(sock, 'cypher', b'', b'done', exit_on_failure=True)

    print()
    common.logging.info('[IntraPartition] Testing getAllProperties with null values')
    common.send_and_expect_response(sock, 'cypher', common.CYPHER, b'Graph ID:', exit_on_failure=True)
    common.send_and_expect_response(sock, 'cypher', b'2', b'Input query :', exit_on_failure=True)
    common.send_and_expect_response(sock, 'cypher', b'MATCH (n:Location) WHERE n.id = 6 RETURN n',
                                    b'{"n":{"category":"Park","id":"6","label":"Location",'
                                    b'"name":"Central Park","partitionID":"0"}}',
                                    exit_on_failure=True)
    common.send_and_expect_response(sock, 'cypher', b'', b'done', exit_on_failure=True)

    print()
    common.logging.info('[IntraPartition] Testing getAllProperties multiple nodes (lifetime safety)')
    common.send_and_expect_response(sock, 'cypher', common.CYPHER, b'Graph ID:', exit_on_failure=True)
    common.send_and_expect_response(sock, 'cypher', b'2', b'Input query :', exit_on_failure=True)
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
    common.send_and_expect_response(sock, 'cypher', common.CYPHER, b'Graph ID:', exit_on_failure=True)
    common.send_and_expect_response(sock, 'cypher', b'4', b'Input query :', exit_on_failure=True)
    sock.sendall(b'MATCH (n) WHERE n.id = 1 RETURN n' + common.LINE_END)
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
        common.logging.info('✓ Large graph node query returned results')
    else:
        common.logging.warning('Large graph query unexpected response: %s', response[:100])
        common.failed_tests.append('[IntraPartition] Large graph getAllProperties')
    common.send_and_expect_response(sock, 'cypher', b'', b'done', exit_on_failure=True)

    print()
    common.logging.info('[IntraPartition] Testing relationship getAllProperties')
    common.send_and_expect_response(sock, 'cypher', common.CYPHER, b'Graph ID:', exit_on_failure=True)
    common.send_and_expect_response(sock, 'cypher', b'4', b'Input query :', exit_on_failure=True)
    sock.sendall(b'MATCH (n)-[r]->(m) WHERE n.id = 1 RETURN n, r, m' + common.LINE_END)
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
        common.logging.info('✓ Relationship query returned results with correct structure')
    else:
        common.logging.warning('Relationship query unexpected response: %s', response[:100])
        common.failed_tests.append('[IntraPartition] Relationship structure')
    common.send_and_expect_response(sock, 'cypher', b'', b'done', exit_on_failure=True)

    print()
    common.logging.info('[Cypher] Testing OrderBy for Large Graph')
    common.send_and_expect_response(sock, 'cypher', common.CYPHER, b'Graph ID:', exit_on_failure=True)
    common.send_and_expect_response(sock, 'cypher', b'4', b'Input query :', exit_on_failure=True)
    common.send_and_expect_response_file(sock, 'cypher', b'MATCH (n) RETURN n.id, n.name, n.code '
                                                         b'ORDER BY n.code ASC',
                                         'tests/integration/utils/expected_output/'
                                         'orderby_expected_output_file.txt', exit_on_failure=True)
