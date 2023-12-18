"""
Copyright 2023 JasmineGraph Team
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
    logging.INFO, f"\033[1;32m{logging.getLevelName(logging.INFO)}\033[1;0m")
logging.addLevelName(
    logging.WARNING, f"\033[1;33m{logging.getLevelName(logging.WARNING)}\033[1;0m")
logging.addLevelName(
    logging.ERROR, f"\033[1;31m{logging.getLevelName(logging.ERROR)}\033[1;0m")
logging.addLevelName(
    logging.CRITICAL, f"\033[1;41m{logging.getLevelName(logging.CRITICAL)}\033[1;0m")

logging.getLogger().setLevel(logging.INFO)

HOST = '127.0.0.1'
PORT = 7777  # The port used by the server

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
LINE_END = b'\r\n'


def expect_response(conn: socket.socket, expected: bytes):
    '''
    Check if the response is equal to the expected response
    Return True if they are equal or False otherwise.
    '''
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
    '''
    Send a message to server and check if the response is equal to the expected response
    Append the test name to failed tests list on failure.
    If exit_on_failure is True, and the response did not match, exit the test script after printing
    the test stats.
    '''
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

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.connect((HOST, PORT))

    print()
    logging.info("Testing lst")
    send_and_expect_response(sock, 'Initial lst', LIST, EMPTY)

    print()
    logging.info("Testing adgr")
    send_and_expect_response(sock, 'adgr', ADGR, SEND, exit_on_failure=True)
    send_and_expect_response(
        sock, 'adgr', b'powergrid|/var/tmp/data/powergrid.dl', DONE, exit_on_failure=True)

    print()
    logging.info("Testing lst after adgr")
    send_and_expect_response(sock, "lst after adgr", LIST,
                             b'|1|powergrid|/var/tmp/data/powergrid.dl|op|')

    print()
    logging.info("Testing ecnt")
    send_and_expect_response(sock, "ecnt", ECNT, b'graphid-send')
    send_and_expect_response(sock, "ecnt", b'1', b'6594')

    print()
    logging.info("Testing vcnt")
    send_and_expect_response(sock, "vcnt", VCNT, b'graphid-send')
    send_and_expect_response(sock, "vcnt", b'1', b'4941')

    print()
    logging.info("Testing trian")
    send_and_expect_response(sock, "trian", TRIAN,
                             b'grap', exit_on_failure=True)
    send_and_expect_response(
        sock, "trian", b'1', b'priority(>=1)', exit_on_failure=True)
    send_and_expect_response(sock, "trian", b'1', b'651')

    print()
    logging.info("Testing pgrnk")
    send_and_expect_response(sock, "pgrnk", PGRNK,
                             b'send', exit_on_failure=True)
    send_and_expect_response(sock, "pgrnk", b'1|0.5|40',
                             DONE, exit_on_failure=True)

    print()
    logging.info("Testing adgr-cust")
    send_and_expect_response(sock, "adgr-cust", ADGR_CUST, b'Select a custom graph upload option' +
                             LINE_END +
                             b'1 : Graph with edge list + text attributes list' + LINE_END +
                             b'2 : Graph with edge list + JSON attributes list' + LINE_END +
                             b'3 : Graph with edge list + XML attributes list',
                             exit_on_failure=True)
    send_and_expect_response(sock, "adgr-cust",
                             b'1',
                             b'Send <name>|<path to edge list>|<path to attribute file>|' +
                             b'(optional)<attribute data type: int8. int16, int32 or float>',
                             exit_on_failure=True)
    send_and_expect_response(sock, "adgr-cust",
                             b'cora|/var/tmp/data/cora/cora.cites|/var/tmp/data/cora/cora.content',
                             DONE, exit_on_failure=True)

    print()
    logging.info("Testing lst after adgr-cust")
    send_and_expect_response(sock, "lst after adgr-cust", LIST,
                             b'|1|powergrid|/var/tmp/data/powergrid.dl|op|' + LINE_END +
                             b'|2|cora|/var/tmp/data/cora/cora.cites|op|')

    print()
    logging.info("Testing merge")
    send_and_expect_response(sock, "merge", MERGE, b'Available main flags:' + LINE_END +
                             b'graph_id' + LINE_END +
                             b'Send --<flag1> <value1>')
    send_and_expect_response(
        sock, "merge", b'--graph_id 2', DONE, exit_on_failure=True)

    print()
    logging.info("Testing train")
    send_and_expect_response(sock, "train", TRAIN, b'Available main flags:' + LINE_END +
                             b'graph_id learning_rate batch_size validate_iter epochs' + LINE_END +
                             b'Send --<flag1> <value1> --<flag2> <value2> ..',
                             exit_on_failure=True)
    send_and_expect_response(
        sock, "train", b'--graph_id 2', DONE, exit_on_failure=True)

    print()
    logging.info("Testing rmgr")
    send_and_expect_response(sock, 'rmgr', RMGR, SEND)
    send_and_expect_response(sock, 'rmgr', b'1', DONE)

    print()
    logging.info("Testing lst after rmgr")
    send_and_expect_response(sock, 'lst after rmgr',
                             LIST, b'|2|cora|/var/tmp/data/cora/cora.cites|op|')

    print()
    logging.info("Shutting down")
    sock.sendall(SHDN + LINE_END)

    if passed_all:
        print()
        logging.info('Passed all tests')
    else:
        print()
        logging.critical('Failed some tests')
        print(*failed_tests, sep='\n', file=sys.stderr)
        sys.exit(1)
