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
import gc
import socket
import pickle
import select
import numpy as np
import pandas as pd

# CONSTANTS
HEADER_LENGTH = 10
WEIGHT_FILE_PATH = '/var/tmp/jasminegraph-localstore/'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s : [%(levelname)s]  %(message)s',
    handlers=[
        logging.FileHandler('aggregator.log'),
        logging.StreamHandler(sys.stdout)
    ]
)


class Aggregator:
    """Aggregator
    """

    def __init__(self, model, rounds, num_orgs, ip, port):

        # Parameters
        self.ip = ip
        self.port = port
        self.num_clients = num_orgs
        self.rounds = rounds

        # Global model
        self.model_weights = model

        self.weights = []
        self.partition_sizes = []
        self.training_cycles = 0

        self.stop_flag = False

        # List of sockets for select.select()
        self.sockets_list = []
        self.clients = {}
        self.client_ids = {}

        # Create Aggregator socket
        self.aggregator_socket = socket.socket(
            socket.AF_INET, socket.SOCK_STREAM)
        self.aggregator_socket.setsockopt(
            socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.aggregator_socket.bind((self.ip, self.port))
        self.aggregator_socket.listen(self.num_clients)

        self.sockets_list.append(self.aggregator_socket)

    def update_model(self, new_weights, num_examples):
        """Update model
        """
        self.partition_sizes.append(num_examples)
        self.weights.append(num_examples * new_weights)

        logging.info(str(self.partition_sizes))
        logging.info(str(self.weights))

        if (len(self.weights) == self.num_clients) and (self.partition_sizes > 0):

            avg_weight = sum(self.weights) / sum(self.partition_sizes)

            self.weights = []
            self.partition_sizes = []

            self.model_weights = avg_weight

            self.training_cycles += 1

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
        if self.rounds == self.training_cycles:
            self.stop_flag = True

        weights = np.array(self.model_weights)
        weights_path = WEIGHT_FILE_PATH + 'weights_round_' + \
            str(self.training_cycles) + '.npy'
        np.save(weights_path, weights)

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
        """Run aggregator
        """
        while not self.stop_flag:
            read_sockets, _, exception_sockets = select.select(
                self.sockets_list, [], self.sockets_list)

            for notified_socket in read_sockets:
                if notified_socket == self.aggregator_socket:
                    client_socket, client_address = self.aggregator_socket.accept()
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
                        continue

                    client_id = message['ORG_ID']
                    weights = message['WEIGHTS']

                    weights_path = WEIGHT_FILE_PATH + 'weights_round_' + \
                        str(self.training_cycles) + '.npy'
                    np.save(weights_path, weights)

                    num_examples = message['NUM_EXAMPLES']
                    self.client_ids[notified_socket] = client_id

                    logging.info('Recieved model from client-%s at %s:%s',
                                 client_id, *self.clients[notified_socket])
                    self.update_model(weights, int(num_examples))

            for notified_socket in exception_sockets:
                self.sockets_list.remove(notified_socket)
                del self.clients[notified_socket]


if __name__ == '__main__':

    from models.supervised import Model

    arg_names = [
        'path_nodes',
        'path_edges',
        'graph_id',
        'partition_id',
        'num_orgs',
        'num_rounds',
        'IP',
        'PORT'
    ]

    args = dict(zip(arg_names, sys.argv[1:]))

    logging.info(
        '################################# New Training Session #################################')
    logging.info('aggregator started , graph ID %s, number of clients %s, number of rounds %s',
                 args['graph_id'], args['num_orgs'], args['num_rounds'])

    if 'IP' not in args.keys() or args['IP'] == 'localhost':
        args['IP'] = 'localhost'

    if 'PORT' not in args.keys():
        args['PORT'] = 5000

    path_nodes = args['path_nodes'] + args['graph_id'] + \
        '_nodes_' + args['partition_id'] + '.csv'
    nodes = pd.read_csv(path_nodes, index_col=0)

    path_edges = args['path_edges'] + args['graph_id'] + \
        '_edges_' + args['partition_id'] + '.csv'
    edges = pd.read_csv(path_edges)

    model = Model(nodes, edges)
    model.initialize()
    model_weights = model.get_weights()

    logging.info('Model initialized')

    aggregator = Aggregator(model_weights, rounds=int(args['num_rounds']),
                            num_orgs=int(args['num_orgs']), ip=args['IP'], port=int(args['PORT']))

    del nodes, edges, model
    gc.collect()

    logging.info('Federated training started!')

    start = timer()
    aggregator.run()
    end = timer()

    elapsed_time = end - start
    logging.info('Federated training done!')
    logging.info(
        'Training report : Elapsed time %s seconds, graph ID %s, number of clients %s, number \
            of rounds %s',
        elapsed_time, args['graph_id'], args['num_orgs'], args['num_rounds'])
