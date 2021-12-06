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

import logging
from operator import mod
import sys

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s : [%(levelname)s]  %(message)s',
    handlers=[
        logging.FileHandler('supervised.log'),
        logging.StreamHandler(sys.stdout)
    ]
)


import stellargraph as sg
from stellargraph.data import EdgeSplitter
from stellargraph.mapper import GraphSAGELinkGenerator
from stellargraph.layer import GraphSAGE, HinSAGE, link_classification
from stellargraph import globalvar
from stellargraph import datasets

import tensorflow as tf
from tensorflow import keras
import random as python_random


from sklearn import preprocessing, feature_extraction, model_selection
import os
import numpy as np
import pandas as pd
from timeit import default_timer as timer

tf.random.set_seed(42)
np.random.seed(42)
python_random.seed(42)

class Model:

    def __init__(self,nodes,edges):
        self.model = None

        self.nodes =  nodes
        self.edges = edges

        self.graph_train = None
        self.graph_test = None

        self.train_flow = None
        self.test_flow = None

    def initialize(self,**hyper_params):

        if(not "batch_size" in hyper_params.keys()):
            batch_size = 20
        if(not "layer_sizes" in hyper_params.keys()):
            num_samples = [20, 10]
        if(not "num_samples" in hyper_params.keys()):
            layer_sizes = [10, 10 ]
        if(not "bias" in hyper_params.keys()):
            bias = True
        if(not "dropout" in hyper_params.keys()):
            dropout = 0.1
        if(not "lr" in hyper_params.keys()):
            lr = 1e-2

        graph = sg.StellarGraph(nodes=self.nodes,edges=self.edges)

        # Test split
        edge_splitter_test = EdgeSplitter(graph)
        self.graph_test, edge_ids_test, edge_labels_test = edge_splitter_test.train_test_split(
            p=0.1, method="global", keep_connected=True, seed = 42
        )

        # Train split
        edge_splitter_train = EdgeSplitter(self.graph_test)
        self.graph_train, edge_ids_train, edge_labels_train = edge_splitter_train.train_test_split(
            p=0.1, method="global", keep_connected=True, seed = 42
        )

        # Train iterators
        train_gen = GraphSAGELinkGenerator(self.graph_train, batch_size, num_samples, seed = 42)
        self.train_flow = train_gen.flow(edge_ids_train, edge_labels_train, shuffle=True)

        # Test iterators
        test_gen = GraphSAGELinkGenerator(self.graph_train, batch_size, num_samples, seed = 42)
        self.test_flow = test_gen.flow(edge_ids_test, edge_labels_test, shuffle=True)

        # Model defining - Keras functional API + Stellargraph layers
        graphsage = GraphSAGE(
            layer_sizes=layer_sizes, generator=train_gen, bias=bias, dropout=dropout
        )

        x_inp, x_out = graphsage.in_out_tensors()

        prediction = link_classification(
            output_dim=1, output_act="sigmoid", edge_embedding_method="ip"
        )(x_out)

        self.model = keras.Model(inputs=x_inp, outputs=prediction)

        self.model.compile(
            optimizer=keras.optimizers.Adam(lr=lr),
            loss=keras.losses.binary_crossentropy,
            metrics=[keras.metrics.BinaryAccuracy(),keras.metrics.Recall(),keras.metrics.AUC(),keras.metrics.Precision()],
        )

        # return number of training and testing examples
        return edge_ids_train.shape[0],edge_ids_test.shape[0]

    def set_weights(self,weights):
        self.model.set_weights(weights)

    def get_weights(self):
        return self.model.get_weights()

    def fit(self,epochs = 20):
        history = self.model.fit(self.train_flow, epochs=epochs, verbose=1)
        return self.model.get_weights(),history
    
    def evaluate(self):
        train_metrics = self.model.evaluate(self.train_flow)
        test_metrics = self.model.evaluate(self.test_flow)

        return train_metrics,test_metrics

    def predict(self,*args):
        return self.model.predict(args)

    def getModel(self):
        return self.model

    def saveModel(self):
        self.model.save('model.h5')


if __name__ == "__main__":

    arg_names = [
        'path_weights',
        'path_nodes',
        'path_edges',
        'epochs'
    ]

    args = dict(zip(arg_names, sys.argv[1:]))

    nodes = pd.read_csv(args["path_nodes"],index_col=0)
    edges = pd.read_csv(args["path_edges"])
    edges = edges.astype({"source":"uint32","target":"uint32"})
    path_weights = args["path_weights"]

    logging.warning('####################################### New Training Session #######################################')

    model = Model(nodes,edges)
    model.initialize()

    logging.info('Training started!')
    start = timer()

    new_weights,history = model.fit()

    end = timer()
    logging.info('Training done!')

    elapsed_time = end -start
    np.save(path_weights, new_weights)
    eval = model.evaluate()

    f1_train = (2 * eval[0][2] * eval[0][4]) / (eval[0][2] + eval[0][4])
    f1_test = (2 * eval[1][2] * eval[1][4]) / (eval[1][2] + eval[1][4])

    model.saveModel()

    logging.info('Training set : accuracy - %s, recall - %s, AUC - %s, F1 - %s, precision - %s',eval[0][1],eval[0][2],eval[0][3],f1_train,eval[0][4])
    logging.info('Testing set : accuracy - %s, recall - %s, AUC - %s, F1 - %s, precision - %s',eval[1][1],eval[1][2],eval[1][3],f1_test,eval[1][4])
    logging.info('Elapsed time : %s seconds',elapsed_time)
