"""Copyright 2025 JasmineGraph Team
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
import subprocess
import sys
import socket
import logging
import os
import time

from KG.test import test_kg

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
SHDN = b'shdn'
SEND = b'send'
DONE = b'done'
ADHDFS = b'adhdfs'
LINE_END = b'\r\n'
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

UPLOAD_SCRIPT = os.path.join(BASE_DIR, 'utils/datasets/upload-hdfs-file.sh')
OLLAMA_SETUP_SCRIPT = os.path.join(BASE_DIR, 'graphRAG/utils/start-ollama.sh')
TEXT_FOLDER = os.path.join(BASE_DIR, 'graphRAG/KG/gold')
def expect_response(conn: socket.socket, expected: bytes, timeout: float = 30000.0):
    """Check if the response is equal to the expected response within a timeout.
    Return True if they are equal, False otherwise.
    """
    global passed_all
    buffer = bytearray()
    read = 0
    expected_len = len(expected)

    deadline = time.time() + timeout  # set overall timeout deadline

    while read < expected_len:
        # check deadline
        if time.time() > deadline:
            logging.warning('Timed out waiting for full response')
            passed_all = False
            return False

        try:
            received = conn.recv(expected_len - read)
        except socket.error as e:
            logging.warning('Socket error: %s', e)
            passed_all = False
            return False

        if not received:
            logging.warning('Connection closed before expected response was fully received')
            passed_all = False
            return False

        received_len = len(received)
        if received != expected[read:read + received_len]:
            buffer.extend(received)
            data = bytes(buffer)
            logging.warning(
                'Output mismatch\nexpected : %s\nreceived : %s',
                expected.decode(), data.decode())
            passed_all = False
            return False

        read += received_len
        buffer.extend(received)

    data = bytes(buffer)
    print(data.decode('utf-8'), end='')
    assert data == expected
    return True


def expect_response_file(conn: socket.socket, expected: bytes, timeout=5000):
    """Check if the response matches expected file."""
    global passed_all
    buffer = bytearray()
    conn.setblocking(False)
    start = time.time()

    while time.time() - start < timeout:
        try:
            received = conn.recv(4096)
            if received:
                buffer.extend(received)
                start = time.time()
                if b'done' in buffer:
                    break
            else:
                time.sleep(0.01)
        except BlockingIOError:
            time.sleep(0.01)

    conn.setblocking(True)
    data = bytes(buffer)

    received_lines = data.decode(errors='replace').splitlines()
    expected_lines = expected.decode(errors='replace').splitlines()

    mismatches = []
    for i, (exp_line, rec_line) in enumerate(zip(expected_lines, received_lines), start=1):
        if exp_line != rec_line:
            mismatches.append(f'Line {i}:\n  expected: {exp_line}\n  received: {rec_line}')

    # Handle extra lines if lengths differ
    if len(received_lines) > len(expected_lines):
        for i in range(len(expected_lines) + 1, len(received_lines) + 1):
            mismatches.append(f'Line {i}:\n  expected: <no line>\n  '
                              f'received: {received_lines[i-1]}')
        logging.warning('Output mismatch! Showing first 10 differences:\n%s',
                        '\n'.join(mismatches[:10]))
        passed_all = False
        return False
    if len(expected_lines) > len(received_lines):
        for i in range(len(received_lines) + 1, len(expected_lines) + 1):
            mismatches.append(f'Line {i}:\n  expected: {expected_lines[i-1]}\n'
                              f'  received: <no line>')
        logging.warning('Output mismatch! Showing first 10 differences:\n%s',
                        '\n'.join(mismatches[:10]))
        passed_all = False
        return False

    print('All the records match')
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

def send_and_expect_response_file(conn, test_name, send, expected_file, exit_on_failure=False):
    """Send a message to server and check the response on-the-fly against a large expected
    response file."""
    conn.sendall(send + LINE_END)
    print(send.decode('utf-8'))
    with open(expected_file, 'rb') as f:
        expected_bytes = f.read()

    if not expect_response_file(conn, expected_bytes):
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

    subprocess.run(['bash', OLLAMA_SETUP_SCRIPT], check=True)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((host, port))
        print()
        logging.info('[GraphRAG] Testing knowledge graph construction ')
        test_kg(TEXT_FOLDER ,UPLOAD_SCRIPT, host, port)



        # shutting down workers after testing
        print()
        logging.info('Shutting down')
        sock.sendall(SHDN + LINE_END)
        if passed_all:
            print()
            logging.info('Passed all tests')
        else:
            print()
            logging.critical('Failed some tests')
            print(*failed_tests, sep='\n', file=sys.stderr)
            sys.exit(1)

if __name__ == '__main__':
    test(HOST, PORT)
