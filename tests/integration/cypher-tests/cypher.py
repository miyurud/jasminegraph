from tests.integration.test import send_and_expect_response


def test_cypher_queries(sock, graph_id='2'):
    """Test Cypher query capabilities of the JasmineGraph server with a given graph ID."""
    def send(query_name, send, expected):
        send_and_expect_response(sock, query_name, send, expected, exit_on_failure=True)

    def cypher_send(query_name, query):
        send(f'[Cypher] {query_name}', b'cypher', b'Graph ID:')
        send(f'[Cypher] {query_name}', str(graph_id).encode(), b'Input query :')
        send(f'[Cypher] {query_name}', query[0], query[1])
        send(f'[Cypher] {query_name}', b'', b'done')

    # Cypher aggregate
    cypher_send('Aggregate Query', (
        b'match (n) where n.id < 10 return avg(n.id)',
        b'{"avg(n.id)":4.5}'
    ))

    # AllNodeScan
    cypher_send('AllNodeScan', (
        b'MATCH (n) WHERE n.id=2 RETURN n',
        b'{"n":{"id":"2","label":"Person","name":"Charlie","occupation":"IT Engineer","partitionID":"0"}}'
    ))

    # ProduceResults
    cypher_send('ProduceResults', (
        b'MATCH (n) WHERE n.id = 18 RETURN n.age, n.name',
        b'{"n.age":null,"n.name":"Skyport Airport"}'
    ))

    # Filter 1
    cypher_send('Filter 1', (
        b"MATCH (n) WHERE n.name = 'Fiona' RETURN n",
        b'{"n":{"age":"25","id":"10","label":"Person","name":"Fiona","occupation":"Artist","partitionID":"0"}}'
    ))

    # Filter 2
    cypher_send('Filter 2', (
        b'MATCH (n) WHERE n.age < 30 return n',
        b'{"n":{"age":"25","id":"10","label":"Person","name":"Fiona","occupation":"Artist","partitionID":"0"}}'
    ))

    # Expand All
    cypher_send('Expand All', (
        b'MATCH (a)-[r]-(b)-[d]-(s) WHERE (a.id = 10 AND s.id=14) RETURN a, b, s',
        b'{"a":{"age":"25","id":"10","label":"Person","name":"Fiona","occupation":"Artist","partitionID":"0"},'
        b'"b":{"id":"2","label":"Person","name":"Charlie","occupation":"IT Engineer","partitionID":"0"},'
        b'"s":{"id":"14","label":"Person","name":"Julia","occupation":"Entrepreneur","partitionID":"0"}}'
    ))

    # UndirectedRelationshipTypeScan
    cypher_send('UndirectedRelationshipTypeScan', (
        b"MATCH (n {name:'Eva'})-[:NEIGHBORS]-(x ) RETURN x",
        b'{"x":{"id":"0","label":"Person","name":"Alice","occupation":"Teacher","partitionID":"0"}}'
    ))

    # UndirectedAllRelationshipScan
    cypher_send('UndirectedAllRelationshipScan', (
        b"MATCH (n)-[r]-(m {id:6} ) WHERE n.age = 25 RETURN n, r, m",
        b'{"m":{"category":"Park","id":"6","label":"Location","name":"Central Park","partitionID":"0"},'
        b'"n":{"age":"25","id":"10","label":"Person","name":"Fiona","occupation":"Artist","partitionID":"0"},'
        b'"r":{"description":"Fiona and Central Park have been friends since college.","id":"11","type":"FRIENDS"}}'
    ))

    # Directed Relationship
    cypher_send('Directed Relationship', (
        b"MATCH (n {name:'Eva'})-[:NEIGHBORS]->(x ) RETURN x",
        b'{"x":{"id":"0","label":"Person","name":"Alice","occupation":"Teacher","partitionID":"0"}}'
    ))

    # OrderBy
    send('[Cypher] OrderBy', b'cypher', b'Graph ID:')
    send('[Cypher] OrderBy', str(graph_id).encode(), b'Input query :')
    send('[Cypher] OrderBy', b"match (n) where n.partitionID = '1' return n order by n.name ASC",
         b'{"n":{"category":"Studio","id":"15","label":"Location","name":"Art Studio","partitionID":"1"}}')
    send('[Cypher] OrderBy', b'', b'{"n":{"id":"1","label":"Person","name":"Bob","occupation":"Banker","partitionID":"1"}}')
    send('[Cypher] OrderBy', b'', b'{"n":{"id":"3","label":"Person","name":"David","occupation":"Doctor","partitionID":"1"}}')
    send('[Cypher] OrderBy', b'', b'{"n":{"id":"11","label":"Person","name":"George","occupation":"Chef","partitionID":"1"}}')
    send('[Cypher] OrderBy', b'', b'{"n":{"category":"Restaurant","id":"17","label":"Location","name":"Gourmet Bistro","partitionID":"1"}}')
    send('[Cypher] OrderBy', b'', b'{"n":{"category":"School","id":"5","label":"Location","name":"Greenfield School","partitionID":"1"}}')
    send('[Cypher] OrderBy', b'', b'{"n":{"id":"13","label":"Person","name":"Ian","occupation":"Pilot","partitionID":"1"}}')
    send('[Cypher] OrderBy', b'', b'{"n":{"category":"Coworking Space","id":"19","label":"Location","name":"Innovation Hub","partitionID":"1"}}')
    send('[Cypher] OrderBy', b'', b'{"n":{"category":"Bank","id":"7","label":"Location","name":"Town Bank","partitionID":"1"}}')
    send('[Cypher] OrderBy', b'', b'{"n":{"category":"Hospital","id":"9","label":"Location","name":"Town General Hospital","partitionID":"1"}}')
    send('[Cypher] OrderBy', b'', b'done')

    # NodeScanByLabel
    cypher_send('NodeScanByLabel', (
        b'match(n:Person) where n.id=2 return n RETURN n',
        b'{"n":{"id":"2","label":"Person","name":"Charlie","occupation":"IT Engineer","partitionID":"0"}}'
    ))

if __name__ == "__main__":
    import socket
    import sys

    # Define host and port for the JasmineGraph server
    HOST = 'localhost'
    PORT = 7777

    # Create a socket connection to the server
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((HOST, PORT))
        print(f"Connected to JasmineGraph server at {HOST}:{PORT}")

        # Run the Cypher tests
        test_cypher_queries(sock, 11)
