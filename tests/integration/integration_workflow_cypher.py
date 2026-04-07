"""Cypher-focused integration workflow."""
import os

import integration_common as common


def run_cypher_workflow(sock, host, port):
    """Run cypher and graph upload scenario tests."""
    print()
    common.logging.info('Testing adhdfs for custom graph with properties')
    common.send_and_expect_response(sock, 'adhdfs', common.ADHDFS,
                                    b'Do you want to use the default HDFS server(y/n)?',
                                    exit_on_failure=True)
    common.send_and_expect_response(sock, 'adhdfs', b'n',
                                    b'Send the file path to the HDFS configuration file.' +
                                    b' This file needs to be in some directory location ' +
                                    b'that is accessible for JasmineGraph master',
                                    exit_on_failure=True)
    common.send_and_expect_response(sock, 'adhdfs', b'/var/tmp/config/hdfs_config.txt',
                                    b'HDFS file path: ',
                                    exit_on_failure=True)
    common.send_and_expect_response(sock, 'adhdfs', b'/home/graph_with_properties.txt',
                                    b'Is this an edge list type graph(y/n)?',
                                    exit_on_failure=True)
    common.send_and_expect_response(sock, 'adhdfs', b'n',
                                    b'Is this a directed graph(y/n)?',
                                    exit_on_failure=True)
    common.send_and_expect_response(sock, 'adhdfs', b'y', common.DONE, exit_on_failure=True)

    print()
    common.logging.info('2. Testing cypher aggregate query after adding the graph')
    common.send_and_expect_response(sock, 'cypher', common.CYPHER, b'Graph ID:', exit_on_failure=True)
    common.send_and_expect_response(sock, 'cypher', b'2', b'Input query :', exit_on_failure=True)
    common.send_and_expect_response(sock, 'cypher', b'match (n) where n.id < 10 return avg(n.id)',
                                    b'{"avg(n.id)":4.5}', exit_on_failure=True)
    common.send_and_expect_response(sock, 'cypher', b'', b'done', exit_on_failure=True)

    print()
    common.logging.info('[Cypher] Uploading graph for cypher testing')
    common.send_and_expect_response(sock, 'adhdfs', common.ADHDFS,
                                    b'Do you want to use the default HDFS server(y/n)?',
                                    exit_on_failure=True)
    common.send_and_expect_response(sock, 'adhdfs', b'n',
                                    b'Send the file path to the HDFS configuration file.' +
                                    b' This file needs to be in some directory location ' +
                                    b'that is accessible for JasmineGraph master',
                                    exit_on_failure=True)
    common.send_and_expect_response(sock, 'adhdfs', b'/var/tmp/config/hdfs_config.txt',
                                    b'HDFS file path: ',
                                    exit_on_failure=True)
    common.send_and_expect_response(sock, 'adhdfs', b'/home/graph_with_properties.txt',
                                    b'Is this an edge list type graph(y/n)?',
                                    exit_on_failure=True)
    common.send_and_expect_response(sock, 'adhdfs', b'n',
                                    b'Is this a directed graph(y/n)?',
                                    exit_on_failure=True)
    common.send_and_expect_response(sock, 'adhdfs', b'y', common.DONE, exit_on_failure=True)

    print()
    common.logging.info('[Cypher] Uploading large graph for cypher testing')
    common.send_and_expect_response(sock, 'adhdfs', common.ADHDFS,
                                    b'Do you want to use the default HDFS server(y/n)?',
                                    exit_on_failure=True)
    common.send_and_expect_response(sock, 'adhdfs', b'n',
                                    b'Send the file path to the HDFS configuration file.' +
                                    b' This file needs to be in some directory location ' +
                                    b'that is accessible for JasmineGraph master',
                                    exit_on_failure=True)
    common.send_and_expect_response(sock, 'adhdfs', b'/var/tmp/config/hdfs_config.txt',
                                    b'HDFS file path: ',
                                    exit_on_failure=True)
    common.send_and_expect_response(sock, 'adhdfs', b'/home/graph_with_properties_large.txt',
                                    b'Is this an edge list type graph(y/n)?',
                                    exit_on_failure=True)
    common.send_and_expect_response(sock, 'adhdfs', b'n',
                                    b'Is this a directed graph(y/n)?',
                                    exit_on_failure=True)
    common.send_and_expect_response(sock, 'adhdfs', b'y', common.DONE, exit_on_failure=True)

    print()
    common.logging.info('[Adhdfs] Testing uploaded graph')
    abs_path = os.path.abspath('tests/integration/env_init/data/graph_with_properties.txt')
    common.test_graph_validation(abs_path, '2', host, port)

    print()
    common.logging.info('[Cypher] Testing AllNodeScan ')
    common.send_and_expect_response(sock, 'cypher', common.CYPHER, b'Graph ID:', exit_on_failure=True)
    common.send_and_expect_response(sock, 'cypher', b'2', b'Input query :', exit_on_failure=True)
    common.send_and_expect_response(sock, 'cypher', b'MATCH (n) WHERE n.id=2 RETURN n ',
                                    b'{"n":{"id":"2","label":"Person","name":"Charlie",'
                                    b'"occupation":"IT Engineer",'
                                    b'"partitionID":"0"}}', exit_on_failure=True)
    common.send_and_expect_response(sock, 'cypher', b'', b'done', exit_on_failure=True)

    print()
    common.logging.info('[Cypher] Testing ProduceResults ')
    common.send_and_expect_response(sock, 'cypher', common.CYPHER, b'Graph ID:', exit_on_failure=True)
    common.send_and_expect_response(sock, 'cypher', b'2', b'Input query :', exit_on_failure=True)
    common.send_and_expect_response(sock, 'cypher', b'MATCH (n) WHERE n.id = 18 RETURN n.age, n.name ',
                                    b'{"n.age":null,"n.name":"Skyport Airport"}',
                                    exit_on_failure=True)
    common.send_and_expect_response(sock, 'cypher', b'', b'done', exit_on_failure=True)

    print()
    common.logging.info('[Cypher] Testing ProduceResults')
    common.send_and_expect_response(sock, 'cypher', common.CYPHER, b'Graph ID:', exit_on_failure=True)
    common.send_and_expect_response(sock, 'cypher', b'2', b'Input query :', exit_on_failure=True)
    common.send_and_expect_response(sock, 'cypher', b'MATCH (n) WHERE n.id = 18 RETURN n.age, n.name ',
                                    b'{"n.age":null,"n.name":"Skyport Airport"}',
                                    exit_on_failure=True)
    common.send_and_expect_response(sock, 'cypher', b'', b'done', exit_on_failure=True)

    print()
    common.logging.info('[Cypher] Testing filter by equality check')
    common.send_and_expect_response(sock, 'cypher', common.CYPHER, b'Graph ID:', exit_on_failure=True)
    common.send_and_expect_response(sock, 'cypher', b'2', b'Input query :', exit_on_failure=True)
    common.send_and_expect_response(sock, 'cypher', b"MATCH (n) WHERE n.name = 'Fiona' RETURN n",
                                    b'{"n":{"age":"25","id":"10","label":"Person",'
                                    b'"name":"Fiona","occupation":"Artist",'
                                    b'"partitionID":"0"}}',
                                    exit_on_failure=True)
    common.send_and_expect_response(sock, 'cypher', b'', b'done', exit_on_failure=True)

    print()
    common.logging.info('[Cypher] Testing filter by comparison of integer attribute')
    common.send_and_expect_response(sock, 'cypher', common.CYPHER, b'Graph ID:', exit_on_failure=True)
    common.send_and_expect_response(sock, 'cypher', b'2', b'Input query :', exit_on_failure=True)
    common.send_and_expect_response(sock, 'cypher', b'MATCH (n) WHERE n.age < 30 return n',
                                    b'{"n":{"age":"25","id":"10","label":"Person",'
                                    b'"name":"Fiona","occupation":"Artist",'
                                    b'"partitionID":"0"}}',
                                    exit_on_failure=True)
    common.send_and_expect_response(sock, 'cypher', b'', b'done', exit_on_failure=True)

    print()
    common.logging.info('[Cypher] Testing expand all ')
    common.send_and_expect_response(sock, 'cypher', common.CYPHER, b'Graph ID:', exit_on_failure=True)
    common.send_and_expect_response(sock, 'cypher', b'2', b'Input query :', exit_on_failure=True)
    common.send_and_expect_response(sock, 'cypher', b'MATCH (a)-[r]-(b)-[d]-(s)'
                                    b' WHERE (a.id = 10 AND s.id=14) RETURN a, b, s',
                                    b'{"a":{"age":"25","id":"10","label":"Person",'
                                    b'"name":"Fiona","occupation":"Artist","partitionID":"0"},'
                                    b'"b":{"id":"2","label":"Person","name":"Charlie",'
                                    b'"occupation":"IT Engineer","partitionID":"0"},'
                                    b'"s":{"id":"14","label":"Person",'
                                    b'"name":"Julia","occupation":"Entrepreneur","partitionID":"0"}}',
                                    exit_on_failure=True)

    common.send_and_expect_response(sock, 'cypher', b'', b'done', exit_on_failure=True)
    print()
    common.logging.info('[Cypher] Testing Undirected Relationship Type Scan')
    common.send_and_expect_response(sock, 'cypher', common.CYPHER, b'Graph ID:', exit_on_failure=True)
    common.send_and_expect_response(sock, 'cypher', b'2', b'Input query :', exit_on_failure=True)
    common.send_and_expect_response(sock, 'cypher', b'MATCH '
                                    b"(n {name:'Eva'})-[:NEIGHBORS]-(x ) RETURN x",
                                    b'{"x":{"id":"0","label":"Person","name":"Alice",'
                                    b'"occupation":"Teacher","partitionID":"0"}}',
                                    exit_on_failure=True)
    common.send_and_expect_response(sock, 'cypher', b'', b'done', exit_on_failure=True)

    print()
    common.logging.info('[Cypher] Testing Undirected All Relationship Scan')
    common.send_and_expect_response(sock, 'cypher', common.CYPHER, b'Graph ID:', exit_on_failure=True)
    common.send_and_expect_response(sock, 'cypher', b'2', b'Input query :', exit_on_failure=True)
    common.send_and_expect_response(sock, 'cypher', b'MATCH (n)-[r]-(m {id:6} ) WHERE n.age = 25'
                                    b' RETURN n, r, m',
                                    b'{"m":{"category":"Park","id":"6","label":"Location",'
                                    b'"name":"Central Park",'
                                    b'"partitionID":"0"},"n":{"age":"25","id":"10","label":"Person",'
                                    b'"name":"Fiona","occupation":"Artist","partitionID":"0"'
                                    b'},"r":{"description":"Fiona and Central Park have'
                                    b' been friends since college.","id":"11",'
                                    b'"type":"FRIENDS"}}',
                                    exit_on_failure=True)
    common.send_and_expect_response(sock, 'cypher', b'', b'done', exit_on_failure=True)

    print()
    common.logging.info('[Cypher] Testing Directed Relationship Type Scan ')
    common.send_and_expect_response(sock, 'cypher', common.CYPHER, b'Graph ID:', exit_on_failure=True)
    common.send_and_expect_response(sock, 'cypher', b'2', b'Input query :',
                                    exit_on_failure=True)
    common.send_and_expect_response(sock, 'cypher', b'MATCH'
                                    b" (n {name:'Eva'})-[:NEIGHBORS]->(x ) RETURN x",
                                    b'{"x":{"id":"0","label":"Person","name":"Alice",'
                                    b'"occupation":"Teacher","partitionID":"0"}}',
                                    exit_on_failure=True)
    common.send_and_expect_response(sock, 'cypher', b'', b'done', exit_on_failure=True)

    print()
    common.logging.info('[Cypher] Testing OrderBy ')
    common.send_and_expect_response(sock, 'cypher', common.CYPHER, b'Graph ID:', exit_on_failure=True)
    common.send_and_expect_response(sock, 'cypher', b'2', b'Input query :', exit_on_failure=True)
    common.send_and_expect_response(sock, 'cypher', b"match (n) where n.partitionID = '1' return n "
                                    b'order by n.name ASC',
                                    b'''{"n":{"category":"Studio","id":"15","label":"Location",'''
                                    b'''"name":"Art Studio","partitionID":"1"}}''',
                                    exit_on_failure=True)
    common.send_and_expect_response(sock, 'cypher', b'',
                                    b'{"n":{"id":"1","label":"Person","name":"Bob","occupation":'
                                    b'"Banker","partitionID":"1"}}', exit_on_failure=True)

    common.send_and_expect_response(sock, 'cypher', b'',
                                    b'{"n":{"id":"3","label":"Person","name":"David","occupation":'
                                    b'"Doctor","partitionID":"1"}}', exit_on_failure=True)

    common.send_and_expect_response(sock, 'cypher', b'', b'{"n":{"id":"11","label":"Person",'
                                                    b'"name":"George","occupation":"Chef",'
                                                    b'"partitionID":"1"}}', exit_on_failure=True)

    common.send_and_expect_response(sock, 'cypher', b'',
                                    b'{"n":{"category":"Restaurant","id":"17","label":"Location",'
                                    b'"name":"Gourmet Bistro","partitionID":"1"}}',
                                    exit_on_failure=True)

    common.send_and_expect_response(sock, 'cypher', b'',
                                    b'{"n":{"category":"School","id":"5","label":"Location",'
                                    b'"name":"Greenfield School","partitionID":"1"}}',
                                    exit_on_failure=True)

    common.send_and_expect_response(sock, 'cypher', b'', b'{"n":{"id":"13","label":"Person",'
                                                    b'"name":"Ian","occupation":"Pilot",'
                                                    b'"partitionID":"1"}}', exit_on_failure=True)

    common.send_and_expect_response(sock, 'cypher', b'',
                                    b'{"n":{"category":"Coworking Space","id":"19","label":'
                                    b'"Location","name":"Innovation Hub","partitionID":"1"}}',
                                    exit_on_failure=True)

    common.send_and_expect_response(sock, 'cypher', b'',
                                    b'{"n":{"category":"Bank","id":"7","label":"Location","name":'
                                    b'"Town Bank","partitionID":"1"}}', exit_on_failure=True)

    common.send_and_expect_response(sock, 'cypher', b'',
                                    b'{"n":{"category":"Hospital","id":"9","label":"Location",'
                                    b'"name":"Town General Hospital","partitionID":"1"}}',
                                    exit_on_failure=True)
    common.send_and_expect_response(sock, 'cypher', b'', b'done', exit_on_failure=True)

    print()
    common.logging.info('[Cypher] Testing Node Scan By Label')
    common.send_and_expect_response(sock, 'cypher', common.CYPHER, b'Graph ID:', exit_on_failure=True)
    common.send_and_expect_response(sock, 'cypher', b'2', b'Input query :', exit_on_failure=True)
    common.send_and_expect_response(sock, 'cypher', b'match(n:Person) where n.id=2 return n'
                                    b' RETURN n', b'{"n":{"id":"2","label":"Person",'
                                    b'"name":"Charlie","occupation":"IT Engineer",'
                                    b'"partitionID":"0"}}',
                                    exit_on_failure=True)

    common.send_and_expect_response(sock, 'cypher', b'', b'done', exit_on_failure=True)

    print()
    common.logging.info('[Cypher] Testing rmgr after adhdfs')
    common.send_and_expect_response(sock, 'rmgr', common.RMGR, common.SEND, exit_on_failure=True)
    common.send_and_expect_response(sock, 'rmgr', b'1', common.DONE, exit_on_failure=True)
    print()
    common.logging.info('Testing rmgr after adhdfs')
    common.send_and_expect_response(sock, 'rmgr', common.RMGR, common.SEND, exit_on_failure=True)
    common.send_and_expect_response(sock, 'rmgr', b'2', common.DONE, exit_on_failure=True)
    common.send_and_expect_response(sock, 'rmgr', common.RMGR, common.SEND, exit_on_failure=True)
    common.send_and_expect_response(sock, 'rmgr', b'3', common.DONE, exit_on_failure=True)
