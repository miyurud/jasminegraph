import json
import numpy as np
import os

import networkx as nx
# from networkx.readwrite import json_graph


def predict(graphID, queryFile, pathToModels, partitionCount):
    # iterate for each partition and read text file relate to numpy and map all files to one file
    # and stack all numpy arrays
    # This should happen only once. At the first time that we collecting the models
    prepareModel(graphID, pathToModels, partitionCount)

    # find the relevant line in numpy array and get dot prdduct
    node_id_map = json.load(open(pathToModels + "/" + graphID + '-embeddings.json'))
    graph_embeddings = np.load(pathToModels + "/" + graphID + '-embeddings.npy')

    query_graph = nx.read_edgelist(queryFile, nodetype=int)
    query_edges = query_graph.edges()
    batch1 = []
    batch2 = []
    for node1, node2 in query_edges:
        batch1.append(node_id_map[node1])
        batch2.append(node_id_map[node2])

    batch1_embeds = np.take(graph_embeddings,batch1)
    batch2_embeds = np.take(graph_embeddings,batch2)

    np.sum(batch1_embeds*batch2_embeds, axis=1)



def prepareModel(graphID, pathToModels, partitionCount):
    key = 0
    dic = {}
    for i in rage(int(partitionCount)):
        folder = graphID + "_model_" + i;
        with open(pathToModels + "/" + folder + "/" + folder + ".txt") as fp:
            for line in fp:
                dic[int(line)] = key
                key += 1
        if os.path.exists(pathToModels + "/" + folder + "/" + folder + ".npy"):
            part_embeddings = np.load(pathToModels + "/" + folder + "/" + folder + ".npy")
            if (i == 0):
                embeddings = part_embeddings
            else:
                embeddings = np.concatenate((embeddings, part_embeddings), axis=0)

    np.save(pathToModels + "/" + graphID + '-embeddings.npy', embeddings)

    with open(pathToModels + "/" + graphID + '-embeddings.json', 'w') as f:
        json.dump(dic, f)
