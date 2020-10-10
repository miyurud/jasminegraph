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

import socket
import pickle
import select
import sys
import numpy as np
import pandas as pd
import logging
from timeit import default_timer as timer
import time

arg_names = [
    'path_weights',
    'path_nodes',
    'path_edges',
    'graph_id',
    'partition_id',
    'epochs',
    'IP',
    'PORT'
]

args = dict(zip(arg_names, sys.argv[1:]))

partition_id = args['partition_id']

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s : [%(levelname)s]  %(message)s',
    handlers=[
        logging.FileHandler(f'client_{partition_id}.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
class Client:
    """
    Federated client that used to train a given graph partition on a given GCN model
    (Without partition sheduling)
    """

    def __init__(self, MODEL, graph_params, weights_path, graph_id, partition_id, epochs = 10, IP = socket.gethostname(), PORT = 5000, HEADER_LENGTH = 10):

        self.HEADER_LENGTH =  HEADER_LENGTH
        self.IP = IP
        self.PORT = PORT

        self.weights_path = weights_path
        self.graph_id = graph_id
        self.partition_id = partition_id
        self.epochs = epochs

        self.graph_params = graph_params

        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        connected = False
        while not connected:
            try:
                self.client_socket.connect((IP, PORT))
            except ConnectionRefusedError:
                time.sleep(5)
            else:
                logging.info('Connected to the server')
                connected = True
        

        self.MODEL = MODEL
        self.STOP_FLAG = False
        self.rounds = 0


    def send_model(self):
        """
        Send local model weights to the server
        :return: None
        """

        weights_path = self.weights_path + 'weights_' + self.graph_id + '_' + self.partition_id + ".npy"

        weights = np.array(self.MODEL.get_weights())

        data = {"CLIENT_ID":self.partition_id,"WEIGHTS":weights,"NUM_EXAMPLES":self.graph_params[0]}

        data = pickle.dumps(data)
        data = bytes(f"{len(data):<{self.HEADER_LENGTH}}", 'utf-8') + data
        self.client_socket.sendall(data)


    def receive(self):
        """
        Recieve global model weights from the server
        :return: success or failure
        """
        try:

            message_header = self.client_socket.recv(self.HEADER_LENGTH)
            if not len(message_header):
                return False

            message_length = int(message_header.decode('utf-8').strip())

            full_msg = b''
            while True:
                msg = self.client_socket.recv(message_length)

                full_msg += msg

                if len(full_msg) == message_length:
                    break
            
            data = pickle.loads(full_msg)

            self.STOP_FLAG = data["STOP_FLAG"]

            return data["WEIGHTS"]

        except Exception as e:
            logging.error(e)


    def fetch_model(self):
        data = self.receive()
        self.MODEL.set_weights(data)

    def train(self):
        self.MODEL.fit(epochs = self.epochs)

    def run(self):
        """
        Training loop
        """

        while not self.STOP_FLAG:

            read_sockets, _, exception_sockets = select.select([self.client_socket], [], [self.client_socket])

            for soc in read_sockets:
                self.fetch_model()
                
        
            if self.STOP_FLAG:
                eval = self.MODEL.evaluate()

                try:
                    f1_train = (2 * eval[0][2] * eval[0][4]) / (eval[0][2] + eval[0][4])
                    f1_test = (2 * eval[1][2] * eval[1][4]) / (eval[1][2] + eval[1][4])
                except ZeroDivisionError as e:
                    f1_train = "undefined"
                    f1_test = "undefined"
                    
                logging.info('_____________________________________________________ Final model evalution ____________________________________________________________')
                logging.info('Finel model (v%s) fetched',self.rounds)
                logging.info('Training set : loss - %s, accuracy - %s, recall - %s, AUC - %s, F1 - %s, precision - %s', eval[0][0], eval[0][1],eval[0][2],eval[0][3],f1_train,eval[0][4])
                logging.info('Testing set : loss - %s, accuracy - %s, recall - %s, AUC - %s, F1 - %s, precision - %s',  eval[1][0], eval[1][1],eval[1][2],eval[1][3],f1_test,eval[1][4])
                
            else:
                
                self.rounds += 1
                logging.info('_____________________________________________________ Training Round %s ____________________________________________________________',self.rounds)
                logging.info('Global model v%s fetched',self.rounds - 1)

                eval = self.MODEL.evaluate()

                try:
                    f1_train = (2 * eval[0][2] * eval[0][4]) / (eval[0][2] + eval[0][4])
                    f1_test = (2 * eval[1][2] * eval[1][4]) / (eval[1][2] + eval[1][4])
                except ZeroDivisionError as e:
                    f1_train = "undefined"
                    f1_test = "undefined"

                logging.info('Global model v%s - Training set evaluation : loss - %s, accuracy - %s, recall - %s, AUC - %s, F1 - %s, precision - %s',self.rounds - 1, eval[0][0], eval[0][1],eval[0][2],eval[0][3],f1_train,eval[0][4])
                logging.info('Global model v%s - Testing set evaluation : loss - %s, accuracy - %s, recall - %s, AUC - %s, F1 - %s, precision - %s',self.rounds - 1,  eval[1][0], eval[1][1],eval[1][2],eval[1][3],f1_test,eval[1][4])

                
                logging.info('Training started')
                self.train()
                logging.info('Training done')

                logging.info('Sent local model to the server')
                self.send_model()


if __name__ == "__main__":

    from models.supervised import Model

    if 'IP' not in args.keys()  or args['IP'] == 'localhost':
        args['IP'] = socket.gethostname()

    if 'PORT' not in args.keys():
        args['PORT'] = 5000

    if 'epochs' not in args.keys():
        args['epoch'] = 10

    logging.warning('####################################### New Training Session #######################################')
    logging.info('Client started, graph ID %s, partition ID %s, epochs %s',args['graph_id'],args['partition_id'],args['epochs'])

    path_nodes = args['path_nodes'] + args['graph_id'] + '_nodes_' + args['partition_id'] + ".csv"
    nodes = pd.read_csv(path_nodes,index_col=0)

    path_edges = args['path_edges'] + args['graph_id'] + '_edges_' + args['partition_id'] + ".csv"
    edges = pd.read_csv(path_edges)

    logging.info('Model initialized')
    model = Model(nodes,edges)
    num_train_ex,num_test_ex = model.initialize()

    graph_params = (num_train_ex,num_test_ex)

    logging.info('Number of training examples - %s, Number of testing examples %s',num_train_ex,num_test_ex)

    client = Client(model,graph_params,weights_path=args['path_weights'],graph_id=args['graph_id'],partition_id=args['partition_id'],epochs = int(args['epochs']) ,IP=args['IP'],PORT=int(args['PORT']))


    logging.info('Federated training started!')

    start = timer()
    client.run()
    end = timer()

    elapsed_time = end -start
    logging.info('Federated training done!')
    logging.info('Training report : Elapsed time %s seconds, graph ID %s, partition ID %s, epochs %s',elapsed_time,args['graph_id'],args['partition_id'],args['epochs'])
    