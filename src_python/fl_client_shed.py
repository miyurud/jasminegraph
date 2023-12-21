"""
Copyright 2020 JasmineGraph Team
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
import gc
import time
from timeit import default_timer as timer
import select
import pickle
import socket
import numpy as np
import pandas as pd
from models.supervised import Model

arg_names = [
    'client_id',
    'path_weights',
    'path_nodes',
    'path_edges',
    'graph_id',
    'partition_ids',
    'epochs',
    'IP',
    'PORT'
]

args = dict(zip(arg_names, sys.argv[1:]))

client_id = args['client_id']

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s : [%(levelname)s]  %(message)s',
    handlers=[
        logging.FileHandler(f'client_shed_{client_id}.log'),
        logging.StreamHandler(sys.stdout)
    ]
)


class Client:
    """
    Federated client that used to train a given list of graph partitions(By partition scheduler)
    on a given GCN model (With partition sheduling)
    """

    def __init__(self, client_id, weights_path, graph_id, partition_ids, epochs=10,
                 ip=socket.gethostname(), port=5000, header_length=10):

        self.header_length = header_length
        self.ip = ip
        self.port = port
        self.client_id = client_id

        self.weights_path = weights_path
        self.graph_id = graph_id
        self.partition_ids = partition_ids
        self.epochs = epochs

        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        connected = False
        while not connected:
            try:
                self.client_socket.connect((ip, port))
            except ConnectionRefusedError:
                time.sleep(5)
            else:
                logging.info('Connected to the server')
                connected = True

        self.global_model = None
        self.model = None

        self.local_models = []
        self.partition_sizes = []

        self.stop_flag = False
        self.rounds = 0

    def send_models(self):
        """
        Send local model weights to the server
        :return: None
        """

        data = {"CLIENT_ID": self.client_id, "PARTITIONS": self.partition_ids,
                "PARTITION_SIEZES": self.partition_sizes, "WEIGHTS": self.local_models}

        data = pickle.dumps(data)
        data = bytes(f"{len(data):<{self.header_length}}", 'utf-8') + data
        self.client_socket.sendall(data)

        self.local_models = []
        self.partition_sizes = []

    def fetch_model(self):
        """
        Recieve global model weights from the server
        :return: success or failure
        """

        message_header = self.client_socket.recv(self.header_length)

        if len(message_header) == 0:
            return False

        message_length = int(message_header.decode('utf-8').strip())

        full_msg = b''
        while True:
            msg = self.client_socket.recv(message_length)

            full_msg += msg

            if len(full_msg) == message_length:
                break

        data = pickle.loads(full_msg)

        self.stop_flag = data["STOP_FLAG"]

        self.global_model = data["WEIGHTS"]

        return True

    def run(self):
        """
        Training loop
        """

        while not self.stop_flag:
            read_sockets, _, _ = select.select(
                [self.client_socket], [], [self.client_socket])

            success = False
            for _ in read_sockets:
                success = self.fetch_model()

                if success:
                    self.rounds += 1
                    logging.info('Global model v%s fetched', self.rounds - 1)
                else:
                    logging.error('Global model fetching failed')

            if not success:
                logging.error('Stop training')
                break

            if self.stop_flag:
                self.model.set_weights(self.global_model)

                logging.info(
                    '___________________________ Final model evalution __________________________')

                for partition in self.partition_ids:
                    logging.info(
                        '**************************** Partition - %s ****************************',
                        partition)
                    eval_result = self.model.evaluate()

                    f1_train = (2 * eval_result[0][2] * eval_result[0][4]
                                ) / (eval_result[0][2] + eval_result[0][4])
                    f1_test = (2 * eval_result[1][2] * eval_result[1][4]
                               ) / (eval_result[1][2] + eval_result[1][4])

                    logging.info('Final model (v%s) fetched', self.rounds)
                    logging.info('Training set : accuracy - %s, recall - %s, AUC - %s, \
                        F1 - %s, precision - %s', eval_result[0][1], eval_result[0][2],
                                 eval_result[0][3], f1_train, eval_result[0][4])
                    logging.info('Testing set : accuracy - %s, recall - %s, AUC - %s, \
                        F1 - %s, precision - %s', eval_result[1][1], eval_result[1][2],
                                 eval_result[1][3], f1_test, eval_result[1][4])

            else:
                logging.info(
                    '_____________________________ Training Round %s ____________________________',
                    self.rounds)

                for partition in self.partition_ids:
                    logging.info(
                        '***************************** Partition - %s ***************************',
                        partition)

                    path_nodes = args['path_nodes'] + \
                        args['graph_id'] + '_nodes_' + partition + ".csv"
                    nodes = pd.read_csv(path_nodes, index_col=0)
                    nodes = nodes.astype("uint8")

                    path_edges = args['path_edges'] + \
                        args['graph_id'] + '_edges_' + partition + ".csv"
                    edges = pd.read_csv(path_edges)
                    edges = edges.astype(
                        {"source": "uint32", "target": "uint32"})

                    logging.info('Model initialized')
                    self.model = Model(nodes, edges)
                    num_train_ex, num_test_ex = self.model.initialize()
                    self.partition_sizes.append(num_train_ex)

                    self.model.set_weights(self.global_model)

                    logging.info(
                        'Number of training examples - %s, Number of testing examples %s',
                        num_train_ex, num_test_ex)

                    eval_result = self.model.evaluate()

                    f1_train = (2 * eval_result[0][2] * eval_result[0][4]
                                ) / (eval_result[0][2] + eval_result[0][4])
                    f1_test = (2 * eval_result[1][2] * eval_result[1][4]
                               ) / (eval_result[1][2] + eval_result[1][4])
                    logging.info('Global model v%s - Training set evaluation : accuracy - %s, \
                        recall - %s, AUC - %s, F1 - %s, precision - %s',
                                 self.rounds -
                                 1, eval_result[0][1], eval_result[0][2], eval_result[0][3],
                                 f1_train, eval_result[0][4])
                    logging.info('Global model v%s - Testing set evaluation : accuracy - %s, \
                        recall - %s, AUC - %s, F1 - %s, precision - %s',
                                 self.rounds - 1,
                                 eval_result[1][1], eval_result[1][2], eval_result[1][3],
                                 f1_test, eval_result[1][4])

                    logging.info('Training started')
                    self.model.fit(epochs=self.epochs)
                    self.local_models.append(
                        np.array(self.model.get_weights()))
                    logging.info('Training done')

                    del self.model, nodes, edges

                    gc.collect()

                logging.info(
                    '************************** All partitions trained **************************')

                logging.info('Sent local models to the aggregator')
                self.send_models()


if __name__ == "__main__":

    if 'IP' not in args.keys() or args['IP'] == 'localhost':
        args['IP'] = socket.gethostname()

    if 'PORT' not in args.keys():
        args['PORT'] = 5000

    if 'epochs' not in args.keys():
        args['epoch'] = 10

    logging.warning(
        '################################# New Training Session #################################')
    logging.info('Client started, graph ID %s, partition IDs %s , epochs %s',
                 args['graph_id'], args['partition_ids'], args['epochs'])

    client = Client(args['client_id'], weights_path=args['path_weights'],
                    graph_id=args['graph_id'], partition_ids=args['partition_ids'].split(
                        ","),
                    epochs=int(args['epochs']), ip=args['IP'], port=int(args['PORT']))

    logging.info('Federated training started!')

    start = timer()
    client.run()
    end = timer()

    elapsed_time = end - start
    logging.info('Federated training done!')
    logging.info(
        'Training report : Elapsed time %s seconds, graph ID %s, partition IDs %s, epochs %s',
        elapsed_time, args['graph_id'], args['partition_ids'], args['epochs'])
