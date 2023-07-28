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

logging.getLogger().setLevel(logging.INFO)

HOST = '127.0.0.1'
PORT = 7777 # The port used by the server

LIST=b'lst'
ADGR=b'adgr'
EMPTY=b'empty'
RMGR=b'rmgr'
VCNT=b'vcnt'
ECNT=b'ecnt'
TRIAN=b'trian'
SHDN=b'shdn'
SEND=b'send'
DONE=b'done'
LINE_END=b'\r\n'

def recv_all(sock, n_bytes):
    buffer = bytearray()
    read = 0
    while read < n_bytes:
        received = sock.recv(n_bytes - read)
        if received:
            read += len(received)
            buffer.extend(received)
    return bytes(buffer)

def expect_response(sock, expected):
    global passedAll
    data = recv_all(sock, len(expected))
    print(data.decode('utf-8'), end='')
    if data != expected:
        logging.warning(f'Output mismatch\nexpected : {expected}\nreceived : {data}')
        passedAll = False
        return False
    return True

def send_and_expect_response(sock, testName, send, expected, exitOnFail=False):
    global failedTests
    sock.sendall(send + LINE_END)
    print(send.decode('utf-8'))
    if not expect_response(sock, expected + LINE_END):
        failedTests.append(testName)
        if exitOnFail:
            print()
            logging.fatal('Failed some tests,', file=sys.stderr)
            print(*failedTests, sep='\n', file=sys.stderr)
            sys.exit(1)

passedAll = True
failedTests = []

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.connect((HOST, PORT))

    print()
    logging.info("Testing lst")
    send_and_expect_response(sock, 'Initial lst', LIST, EMPTY)

    print()
    logging.info("Testing adgr")
    send_and_expect_response(sock, 'adgr', ADGR, SEND, exitOnFail=True)
    send_and_expect_response(sock, 'adgr', b'powergrid|/var/tmp/data/powergrid.dl', DONE, exitOnFail=True)

    print()
    logging.info("Testing lst after adgr")
    send_and_expect_response(sock, "lst after adgr", LIST, b'|1|powergrid|/var/tmp/data/powergrid.dl|op|')

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
    send_and_expect_response(sock, "trian", TRIAN, b'grap', exitOnFail=True)
    send_and_expect_response(sock, "trian", b'1', b'priority(>=1)', exitOnFail=True)
    send_and_expect_response(sock, "trian", b'1', b'651')

    print()
    logging.info("Testing rmgr")
    send_and_expect_response(sock, 'rmgr', RMGR, SEND)
    send_and_expect_response(sock, 'rmgr', b'1', DONE)

    print()
    logging.info("Testing lst after rmgr")
    send_and_expect_response(sock, 'lst after rmgr', LIST, EMPTY)

    print()
    logging.info("Shutting down")
    sock.sendall(SHDN + LINE_END)

    if passedAll:
        print()
        logging.info('Passed all tests')
    else:
        print()
        logging.critical('Failed some tests')
        print(*failedTests, sep='\n', file=sys.stderr)