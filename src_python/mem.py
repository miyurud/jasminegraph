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


def mem(num_of_nodes, num_of_edges, num_of_features, feature_data_type, edge_data_type):
    """
    This function is used for estimating memory usage of a given graph partition
    :param num_of_nodes: number of nodes in the graph partition
    :param num_of_edges: number of edges in the graph partition
    :param num_of_features: number of features in the graph partition
    :param feature_data_type: data type used for store features (input 8 for int8, 64 for int64 etc)
    :param edge_data_type: data type used for store edges (input 8 for int8, 64 for int64 etc)
    :return: estimated memory usage during training of the given partition
    """

    edge_mem = 2 * num_of_edges * (edge_data_type/8)
    node_mem = num_of_nodes * num_of_features * (feature_data_type/8)

    graph_size = (edge_mem + node_mem) / (1024*1024*1024)

    # Empirically we found out that in-memory-graph-size has a linear relationship with the memory usage during training
    # We determined the constatnts using linear regression
    # Memory usage = 3.6 * in-memory-graph-size in Gb + 2
    return 3.6 * graph_size + 2


def mem_est(partition_data, num_of_features, feature_data_type, edge_data_type):
    """
    This function is used for estimating memory usage of given list of graph partitions
    :partiton_data: list of tuples (num_of_nodes,num_of_edges)
    :param feature_data_type: data type used for store features (input 8 for int8, 64 for int64 etc)
    :param edge_data_type: data type used for store edges (input 8 for int8, 64 for int64 etc)
    :return: estimated memory usage during training for the given partition list
    """

    mems = []

    for data in partition_data:
        mems.append(mem(data[0], data[1], num_of_features,
                    feature_data_type, edge_data_type))

    return mems


if __name__ == "__main__":

    # num_of_features
    num_of_features = 1433

    # feature_data_type = int8
    feature_data_type = 8

    # edge_data_type = int64
    edge_data_type = 64

    # partiton_data = list of tuples (num_of_nodes,num_of_edges)
    partition_data = [(1452, 2383), (1432, 2593)]

    mems = mem_est(partition_data, num_of_features,
                   feature_data_type, edge_data_type)

    print(mems)
