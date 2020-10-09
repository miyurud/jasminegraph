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


def mem(a,b,num_of_nodes,num_of_edges,num_of_features,feature_data_type,edge_data_type):
    
    edge_mem = 2 * num_of_edges * (edge_data_type/8)
    node_mem = num_of_nodes * num_of_features * (feature_data_type/8)

    graph_size = (edge_mem + node_mem) / (1024*1024*1024)

    # y = ax + b
    return a* graph_size + b


def mem_est(partition_data,num_of_features,feature_data_type,edge_data_type):

    mems = []

    for data in partition_data:
        mems.append(mem(3.6,2,data[0],data[1],num_of_features,feature_data_type,edge_data_type))

    return mems

if __name__ == "__main__":
    
    # num_of_features
    num_of_features = 1433

    # feature_data_type = int8
    feature_data_type = 8
    
    # edge_data_type = int64
    edge_data_type = 64

    # partiton_data = list of tuples (num_of_nodes,num_of_edges)
    partition_data = [(1452,2383),(1432,2593)]

    mems = mem_est(partition_data,num_of_features,feature_data_type,edge_data_type)

    print(mems)
