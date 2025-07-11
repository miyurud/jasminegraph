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

import sys
import socket
import logging

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
LINE_END = b'\r\n'
CYPHER = b'cypher'


def expect_response(conn: socket.socket, expected: bytes):
    """Check if the response is equal to the expected response
    Return True if they are equal or False otherwise.
    """
    global passed_all
    buffer = bytearray()
    read = 0
    expected_len = len(expected)
    while read < expected_len:
        received = conn.recv(expected_len - read)
        received_len = len(received)
        if received:
            if received != expected[read:read + received_len]:
                buffer.extend(received)
                data = bytes(buffer)
                logging.warning(
                    'Output mismatch\nexpected : %s\nreceived : %s', expected.decode(),
                    data.decode())
                passed_all = False
                return False
            read += received_len
            buffer.extend(received)
    data = bytes(buffer)
    print(data.decode('utf-8'), end='')
    assert data == expected
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

passed_all = True
failed_tests = []

def test(host, port):
    """Test the JasmineGraph server by sending a series of commands and checking the responses."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((host, port))

        # start measuring time taken to upload
        logging.info('Connected to JasmineGraph server at %s:%d', host, port)
        print('Connected to JasmineGraph server at {}:{}'.format(host, port))
        print()

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
        send_and_expect_response(sock, 'adhdfs', b'/home/graph_data_0.4GB.txt',
                                 b'Is this an edge list type graph(y/n)?',
                                 exit_on_failure=True)
        send_and_expect_response(sock, 'adhdfs', b'n',
                                 b'Is this a directed graph(y/n)?',
                                 exit_on_failure=True)

        send_and_expect_response(sock, 'adhdfs', b'y', DONE, exit_on_failure=True)



if __name__ == '__main__':
    ## meanure time taken to upload
    import time
    start_time = time.time()

    test('localhost', 7777)
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Time taken to upload: {elapsed_time:.2f} seconds")
