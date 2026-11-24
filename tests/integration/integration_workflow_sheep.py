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

import integration_common as common


SHEEP_GRAPH_NAME = b'powergrid_sheep'
SHEEP_GRAPH_FILE = b'/var/tmp/data/powergrid.dl'  # NOSONAR
SHEEP_PARTITION_COUNT = b'2'

# The core workflow leaves a single graph (ID 1), so the sheep graph is assigned ID 2.
# It is removed again at the end so later workflows see the same graph IDs as before.
SHEEP_GRAPH_ID = b'2'
SHEEP_ROW = b'|' + SHEEP_GRAPH_ID + b'|' + SHEEP_GRAPH_NAME + b'|' + SHEEP_GRAPH_FILE + b'|op|'

GRAPH_NAME_PROMPT = b'send graph name'
GRAPH_PATH_PROMPT = b'send graph path'
PARTITION_COUNT_PROMPT = b'send number of partitions'
GRAPH_ID_PROMPT = b'graphid-send'
PRIORITY_PROMPT = b'priority(>=1)'

POWERGRID_VERTEX_COUNT = b'4941'
POWERGRID_EDGE_COUNT = b'6594'
POWERGRID_TRIANGLE_COUNT = b'651'


def run_sheep_workflow(sock, host, port):
    """Partition a graph with sheep, verify it, count triangles on it and remove it."""
    print()
    common.logging.info('Testing sheep')
    common.send_and_expect_response(sock, 'sheep', common.SHEEP, GRAPH_NAME_PROMPT,
                                    exit_on_failure=True)
    common.send_and_expect_response(sock, 'sheep', SHEEP_GRAPH_NAME, GRAPH_PATH_PROMPT,
                                    exit_on_failure=True)
    common.send_and_expect_response(sock, 'sheep', SHEEP_GRAPH_FILE, PARTITION_COUNT_PROMPT,
                                    exit_on_failure=True)
    common.send_and_expect_response(sock, 'sheep', SHEEP_PARTITION_COUNT,
                                    b'sheep partitioning completed for graph ID: ' + SHEEP_GRAPH_ID,
                                    exit_on_failure=True)

    print()
    common.logging.info('Testing lst after sheep')
    common.send_and_expect_response_contains(sock, 'lst after sheep', common.LIST, SHEEP_ROW)

    print()
    common.logging.info('Testing ecnt on sheep graph')
    common.send_and_expect_response(sock, 'sheep ecnt', common.ECNT, GRAPH_ID_PROMPT)
    common.send_and_expect_response(sock, 'sheep ecnt', SHEEP_GRAPH_ID, POWERGRID_EDGE_COUNT)

    print()
    common.logging.info('Testing vcnt on sheep graph')
    common.send_and_expect_response(sock, 'sheep vcnt', common.VCNT, GRAPH_ID_PROMPT)
    common.send_and_expect_response(sock, 'sheep vcnt', SHEEP_GRAPH_ID, POWERGRID_VERTEX_COUNT)

    print()
    common.logging.info('Testing shtrian')
    common.send_and_expect_response(sock, 'shtrian', common.SHTRIAN,
                                    GRAPH_ID_PROMPT, exit_on_failure=True)
    common.send_and_expect_response(sock, 'shtrian', SHEEP_GRAPH_ID, PRIORITY_PROMPT,
                                    exit_on_failure=True)
    common.send_and_expect_response(sock, 'shtrian', b'1', POWERGRID_TRIANGLE_COUNT)

    print()
    common.logging.info('Testing rmgr on sheep graph')
    common.send_and_expect_response(sock, 'sheep rmgr', common.RMGR, common.SEND)
    common.send_and_expect_response(sock, 'sheep rmgr', SHEEP_GRAPH_ID, common.DONE,
                                    exit_on_failure=True)

    print()
    common.logging.info('Testing sheep with invalid partition count')
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as error_sock:
        error_sock.connect((host, port))
        common.send_and_expect_response(error_sock, 'sheep invalid partitions', common.SHEEP,
                                        GRAPH_NAME_PROMPT)
        common.send_and_expect_response(error_sock, 'sheep invalid partitions', SHEEP_GRAPH_NAME,
                                        GRAPH_PATH_PROMPT)
        common.send_and_expect_response(error_sock, 'sheep invalid partitions', SHEEP_GRAPH_FILE,
                                        PARTITION_COUNT_PROMPT)
        common.send_and_expect_response(error_sock, 'sheep invalid partitions', b'0',
                                        b'error: partition count must be a positive integer')
