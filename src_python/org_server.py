"""Copyright 2020 JasmineGraph Team
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
import logging
from timeit import default_timer as timer
import os
import socket
import pickle
import select
import time
import numpy as np

# CONSTANTS
FLAG_PATH = '/var/tmp/jasminegraph-localstore/flag.txt'
WEIGHTS_PATH = '/var/tmp/jasminegraph-localstore/weights.txt'
HEADER_LENGTH = 10

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s : [%(levelname)s]  %(message)s',
    handlers=[
        logging.FileHandler('server.log'),
        logging.StreamHandler(sys.stdout)
    ]
)


class Server:
    """Server
    """

    #pylint: disable=too-many-arguments
    def __init__(self, org_id, model_weights, rounds, num_clients, ip, port):

        # Parameters
        self.ip = ip
        self.port = port
        self.num_clients = num_clients
        self.rounds = rounds

        self.org_id = org_id

        # Global model
        self.model_weights = model_weights

        self.weights = []
        self.partition_sizes = []
        self.training_cycles = 0

        self.stop_flag = False

        # List of sockets for select.select()
        self.sockets_list = []
        self.clients = {}
        self.client_ids = {}

        # Craete server socket
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(
            socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.ip, self.port))
        self.server_socket.listen(self.num_clients)

        self.sockets_list.append(self.server_socket)

    def __delete__(self, _):
        if os.path.exists(FLAG_PATH):
            os.remove(FLAG_PATH)

        if os.path.exists(WEIGHTS_PATH):
            os.remove(WEIGHTS_PATH)

    def update_model(self, new_weights, num_examples):
        """Update model
        """
        self.partition_sizes.append(num_examples)
        self.weights.append(num_examples * new_weights)

        if (len(self.weights) == self.num_clients) and (self.partition_sizes > 0):

            avg_weight = sum(self.weights) / sum(self.partition_sizes)

            self.model_weights = avg_weight

            self.training_cycles += 1

            if self.rounds == self.training_cycles:

                weights = np.array(self.model_weights)
                data = {'ORG_ID': self.org_id, 'WEIGHTS': weights,
                        'NUM_EXAMPLES': sum(self.partition_sizes)}
                data = pickle.dumps(data)
                data = bytes(f'{len(data):<{HEADER_LENGTH}}', 'utf-8') + data

                with open(WEIGHTS_PATH, 'wb') as f:
                    f.write(data)

                with open(FLAG_PATH, 'w', encoding='utf8') as f:
                    f.write('1')

                logging.info('Sent global model to global aggregator')

                while True:
                    time.sleep(5)

                    flag = '1'
                    with open(FLAG_PATH, 'r', encoding='utf8') as f:
                        flag = f.read()

                    data = ''
                    if str(flag).strip() == '0':
                        with open(WEIGHTS_PATH, 'rb') as f:
                            data = f.read()

                        data = pickle.loads(data[HEADER_LENGTH:])
                        self.stop_flag = data['STOP_FLAG']
                        self.model_weights = data['WEIGHTS']
                        self.training_cycles = 0

                        break

                logging.info('Recieved global model from global aggregator')

            self.weights = []
            self.partition_sizes = []

            for soc in self.sockets_list[1:]:
                self.send_model(soc)

            logging.info(
                '____________________________ Training round %s done ____________________________',
                self.training_cycles)

        else:

            logging.error('Invalid patition size')

    def send_model(self, client_socket):
        """Send model to client
        """
        weights = np.array(self.model_weights)

        data = {'STOP_FLAG': self.stop_flag, 'WEIGHTS': weights}

        data = pickle.dumps(data)
        data = bytes(f'{len(data):<{HEADER_LENGTH}}', 'utf-8') + data

        client_socket.sendall(data)

        logging.info('Sent global model to client-%s at %s:%s',
                     self.client_ids[client_socket], *self.clients[client_socket])

    def receive(self, client_socket):
        """Receive from client
        """
        try:
            message_header = client_socket.recv(HEADER_LENGTH)

            if len(message_header) == 0:
                logging.error('Client-%s closed connection at %s:%s',
                              self.client_ids[client_socket], *self.clients[client_socket])
                return False

            message_length = int(message_header.decode('utf-8').strip())

            full_msg = b''
            while True:
                msg = client_socket.recv(message_length)

                full_msg += msg

                if len(full_msg) == message_length:
                    break

            return pickle.loads(full_msg)

        except Exception:
            logging.error('Client-%s closed connection at %s:%s',
                          self.client_ids[client_socket], *self.clients[client_socket])
            return False

    def run(self):
        """Run org server
        """
        while not self.stop_flag:
            read_sockets, _, exception_sockets = select.select(
                self.sockets_list, [], self.sockets_list)

            for notified_socket in read_sockets:

                if notified_socket == self.server_socket:

                    client_socket, client_address = self.server_socket.accept()
                    self.sockets_list.append(client_socket)
                    self.clients[client_socket] = client_address
                    self.client_ids[client_socket] = 'new'

                    logging.info(
                        'Accepted new connection at %s:%s', *client_address)
                    self.send_model(client_socket)

                else:

                    message = self.receive(notified_socket)

                    if message is False:
                        self.sockets_list.remove(notified_socket)
                        del self.clients[notified_socket]
                    else:
                        client_id = message['CLIENT_ID']
                        weights = message['WEIGHTS']
                        num_examples = message['NUM_EXAMPLES']
                        self.client_ids[notified_socket] = client_id

                        logging.info('Recieved model from client-%s at %s:%s',
                                     client_id, *self.clients[notified_socket])
                        self.update_model(weights, int(num_examples))

            for notified_socket in exception_sockets:
                self.sockets_list.remove(notified_socket)
                del self.clients[notified_socket]

        with open(FLAG_PATH, 'w', encoding='utf8') as f:
            f.write('STOP')


if __name__ == '__main__':

    arg_names = [
        'org_id',
        'num_clients',
        'num_rounds',
        'IP',
        'port'
    ]

    args = dict(zip(arg_names, sys.argv[1:]))

    logging.info(
        '################################# New Training Session #################################')
    logging.info('Server started , org ID %s, number of clients %s, number of rounds %s',
                 args['org_id'], args['num_clients'], args['num_rounds'])

    if 'IP' not in args.keys() or args['IP'] == 'localhost':
        args['IP'] = socket.gethostname()

    if 'PORT' not in args.keys():
        args['port'] = 5050

    model_weights = ''
    while True:
        time.sleep(5)

        flag = '1'
        with open(FLAG_PATH, 'r', encoding='utf8') as f:
            flag = f.read()

        data = ''
        logging.info(type(flag))
        if str(flag).strip() == '0':
            with open(WEIGHTS_PATH, 'rb') as f:
                data = f.read()

            data = pickle.loads(data[HEADER_LENGTH:])
            model_weights = data['WEIGHTS']

            break

    server = Server(model_weights=model_weights, org_id=args['org_id'], rounds=int(
        args['num_rounds']),
        num_clients=int(args['num_clients']), ip=args['IP'], port=int(args['port']))

    logging.info('Federated training started!')
    start = timer()
    server.run()
    end = timer()

    elapsed_time = end - start
    logging.info('Federated training done!')
    logging.info('Training report : Elapsed time %s seconds, graph ID %s, number of clients %s, \
        number of rounds %s',
                 elapsed_time, args['org_id'], args['num_clients'], args['num_rounds'])
