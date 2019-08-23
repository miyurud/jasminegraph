from __future__ import division
from __future__ import print_function

import json
import numpy as np
import os
import sys

def prepareModel(graphID, vertexCount, pathToModels, partitionCount):
    if not os.path.exists(pathToModels + "/" + graphID):
        os.makedirs(pathToModels + "/" + graphID)
    key = 0
    nodeToIndex = {}
    indexToNode = {}
    vertexCount = int(vertexCount)
    for i in range(int(partitionCount)):
        folder = graphID + "_model_" + str(i);
        with open(pathToModels + "/" + folder + "/" + folder + ".txt") as fp:
            for line in fp:
                nodeToIndex[int(line)] = key
                indexToNode[key] = int(line)
                key += 1
        if os.path.exists(pathToModels + "/" + folder + "/" + folder + ".npy"):
            part_embeddings = np.load(pathToModels + "/" + folder + "/" + folder + ".npy")
            if (i == 0):
                embeddings = part_embeddings
            else:
                embeddings = np.concatenate((embeddings, part_embeddings), axis=0)
        with open(pathToModels + "/"+graphID+"/" + graphID + "_TEST_EDGE_SET.txt", "a+") as fp:
            with open(pathToModels + "/" + folder + "/" + graphID + "_TEST_EDGE_SET_"+str(i)+".txt") as f:
                for line in f:
                    fp.write(line)

    if(len(indexToNode)<vertexCount):
        for j in range(int(partitionCount)):
            folder = graphID + "_model_" + str(j);
            if(os.path.exists(os.path.dirname(pathToModels + "/" + folder + "/" +graphID + "_central_model_" + str(j) + ".txt"))):
                index = 0
                part_embeddings = np.load(pathToModels + "/" + folder + "/" +graphID + "_central_model_" + str(j)+ ".npy")
                with open(pathToModels + "/" + folder + "/" +graphID + "_central_model_" + str(j) + ".txt") as fp:
                    for line in fp:
                        if(int(line) in nodeToIndex.keys()):
                            continue
                        else:
                            nodeToIndex[int(line)] = key
                            indexToNode[key] = int(line)
                            key += 1
                            embeddings = np.concatenate((embeddings, [part_embeddings[index]]), axis=0)
                        index+=1
                        if(len(indexToNode)==vertexCount):
                            break
            if(len(indexToNode)==vertexCount):
                break

    elif(len(indexToNode)==vertexCount):
        print("All nodes are covered by local stores")
    else:
        print("Error")

    print(len(indexToNode))
    print(len(embeddings))

    np.save(pathToModels + "/"+graphID+"/" + graphID + '-embeddings.npy', embeddings)

    with open(pathToModels + "/"+graphID+"/" + graphID + '-embeddings.json', 'w') as f:
        json.dump(nodeToIndex, f)

    with open(pathToModels + "/"+graphID+"/" + graphID + '-idxtoid.json', 'w') as f:
        json.dump(indexToNode, f)


if __name__ == "__main__":
    if len(sys.argv) == 5:
        prepareModel(sys.argv[1],sys.argv[2],sys.argv[3],sys.argv[4])
    else:
        print("usage:  python predict.py <args>")
        # python -m ensemble <graph_id> <vertex count in full graph> </var/tmp/jasminegraph-localstore/jasminegraph-local_trained_model_store> <partition count>