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

import stellargraph as sg
from stellargraph.data import UniformRandomWalk, UnsupervisedSampler
from stellargraph.mapper import GraphSAGELinkGenerator, GraphSAGENodeGenerator
from stellargraph.layer import GraphSAGE, link_classification
from stellargraph import globalvar
from stellargraph import datasets

from tensorflow import keras
from sklearn import preprocessing, feature_extraction, model_selection
import os
import sys
import numpy as np
import pandas as pd

class Model:

    def __init__(self,nodes,edges):
        self.model = None
        self.embedding_model = None

        self.nodes_df =  nodes
        self.edges_df = edges
        self.graph = None
        self.node_gen = None

        self.train_flow = None

    def initialize(self,**hyper_params):

        if(not "batch_size" in hyper_params.keys()):
            batch_size = 16
        if(not "layer_sizes" in hyper_params.keys()):
            num_samples = [25, 10]
        if(not "num_samples" in hyper_params.keys()):
            layer_sizes = [256, 256]
        if(not "bias" in hyper_params.keys()):
            bias = True
        if(not "dropout" in hyper_params.keys()):
            dropout = 0.0
        if(not "lr" in hyper_params.keys()):
            lr = 1e-3
        if(not "num_walks" in hyper_params.keys()):
            num_walks = 1
        if(not "length" in hyper_params.keys()):
            length = 5

        self.graph = sg.StellarGraph(nodes=self.nodes_df,edges=self.edges_df)
        self.nodes = list(self.graph.nodes())

        del self.nodes_df
        del self.edges_df

        unsupervised_samples = UnsupervisedSampler(
            self.graph, nodes=self.nodes, length=length, number_of_walks=num_walks
        )

        # Train iterators
        train_gen = GraphSAGELinkGenerator(self.graph, batch_size, num_samples)
        self.train_flow = train_gen.flow(unsupervised_samples)

        # Model defining - Keras functional API + Stellargraph layers
        graphsage = GraphSAGE(
            layer_sizes=layer_sizes, generator=train_gen, bias=bias, dropout=dropout, normalize="l2"
        )

        x_inp, x_out = graphsage.in_out_tensors()

        prediction = link_classification(
            output_dim=1, output_act="sigmoid", edge_embedding_method="ip"
        )(x_out)

        self.model = keras.Model(inputs=x_inp, outputs=prediction)

        self.model.compile(
            optimizer=keras.optimizers.Adam(lr=lr),
            loss=keras.losses.binary_crossentropy,
            metrics=[keras.metrics.binary_accuracy],
        )

        x_inp_src = x_inp[0::2]
        x_out_src = x_out[0]
        self.embedding_model = keras.Model(inputs=x_inp_src, outputs=x_out_src)

        self.node_gen = GraphSAGENodeGenerator(self.graph, batch_size, num_samples).flow(self.nodes)

        return self.model.get_weights()

    def set_weights(self,weights):
        self.model.set_weights(weights)

    def get_weights(self):
        return self.model.get_weights()

    def fit(self,epochs = 4):
        history = self.model.fit(
            self.train_flow, 
            epochs=epochs, 
            verbose=1,
            use_multiprocessing=False,
            workers=1
        )
        return self.model.get_weights(),history

    def gen_embeddings(self):
        node_embeddings = self.embedding_model.predict(self.node_gen, workers=4, verbose=1)
        return pd.DataFrame(node_embeddings,index=self.nodes)


if __name__ == "__main__":

    path_weights = "./weights/weights.npy"
    path_node_partition = "./data/4_nodes_0.csv"
    path_edge_partition = "./data/4_edges_0.csv"

    nodes = pd.read_csv(path_node_partition, index_col=0 )

    edges = pd.read_csv(path_edge_partition)

    model = Model(nodes,edges)
    model.initialize()

    print("Training started")
    new_weights,history = model.fit()
    print("Training done")

    emb = model.gen_embeddings()
    emb.to_csv("emb.csv")

    # Save weights
    np.save(path_weights,new_weights)
