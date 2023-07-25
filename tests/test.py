import sys
import socket

HOST = '127.0.0.1'
PORT = 7777 # The port used by the server

def recv_all(sock, n):
    b = bytearray()
    read = 0
    while read < n:
        tmp = sock.recv(n-read)
        if tmp:
            read += len(tmp)
            b.extend(tmp)
    return bytes(b)

def expect_response(sock, expected):
    global passedAll
    data = recv_all(s, len(expected))
    print(data.decode('utf-8'), end='')
    if data != expected:
        print(f'Output mismatch\nexpected : {expected}\nreceived : {data}', file=sys.stderr)
        passedAll = False
        return False
    return True

def send_and_expect_response(sock, send, expected, exitOnFail=False):
    s.sendall(send + b'\r\n')
    print(send.decode('utf-8'))
    if not expect_response(sock, expected + b'\r\n'):
        if exitOnFail:
            sys.exit(1)

passedAll = True

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect((HOST, PORT))

    print("\nTesting lst")
    send_and_expect_response(s, b'lst', b'empty')

    print("\nTesting adgr")
    send_and_expect_response(s, b'adgr', b'send', exitOnFail=True)
    send_and_expect_response(s, b'powergrid|/var/tmp/data/powergrid.dl', b'done', exitOnFail=True)

    print("\nTesting lst after adgr")
    send_and_expect_response(s, b'lst', b'|1|powergrid|/var/tmp/data/powergrid.dl|op|')

    print("\nTesting ecnt")
    send_and_expect_response(s, b'ecnt', b'graphid-send')
    send_and_expect_response(s, b'1', b'6594')

    print("\nTesting vcnt")
    send_and_expect_response(s, b'vcnt', b'graphid-send')
    send_and_expect_response(s, b'1', b'4941')

    print("\nTesting trian")
    send_and_expect_response(s, b'trian', b'grap', exitOnFail=True)
    send_and_expect_response(s, b'1', b'priority(>=1)', exitOnFail=True)
    send_and_expect_response(s, b'1', b'651')

    print("\nTesting rmgr")
    send_and_expect_response(s, b'rmgr', b'send')
    send_and_expect_response(s, b'1', b'done')

    print("\nTesting lst after rmgr")
    send_and_expect_response(s, b'lst', b'empty')

    print("\nShutting down")
    s.sendall(b'shdn\r\n')

    if passedAll:
        print('\nPassed all tests')
    else:
        print('\nFailed some tests', file=sys.stderr)