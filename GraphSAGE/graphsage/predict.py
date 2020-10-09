



from __future__ import division
from __future__ import print_function

import json
import numpy as np
import os
import sys
import lshash.lshash


def predict(graphID, vertexCount, queryFile, pathToModels, partitionCount):

    prepareModel(graphID, vertexCount, pathToModels, partitionCount)

    # find the relevant line in numpy array and get dot product
    node_idx_map = json.load(open(pathToModels + "/" + graphID + '-embeddings.json'))
    idx_node_map = json.load(open(pathToModels + "/" + graphID + '-idxtoid.json'))
    graph_embeddings = np.load(pathToModels + "/" + graphID + '-embeddings.npy')

    lsh = lshash.lshash.LSHash(8,256,num_hashtables=32)
    print(lsh)
    for k in range(len(graph_embeddings)):
        lsh.index(graph_embeddings[k],k)

    query = {}
    final_result ={}
    with open(queryFile) as fp:
        for line in fp:
            entry = line.split(",")
            if(len(entry)>2):
                print("Format not recognized")
            else:
                if(len(entry)==1):
                    query[entry[0]] = "all"
                else:
                    query[entry[0]] = entry[1].strip('\n')

    for key,value in query.items():
        query_embed = graph_embeddings[node_idx_map[(key)]]
        if value == "all":
            results = lsh.query(query_embed)
            print(len(results))
        else:
            results = lsh.query(query_embed,int(value)+1)
            print(len(results))
        out = []
        for j in range(len(results)):
            out.append(idx_node_map[str(results[j][-2][-1])])
        final_result[key] = out
    print(final_result)
    with open(pathToModels + "/" + graphID + '-result.json', 'w') as f:
        json.dump(final_result, f)

def prepareModel(graphID, vertexCount, pathToModels, partitionCount):
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
        with open(pathToModels + "/" + graphID + "_TEST_EDGE_SET.txt", "a+") as fp:
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
    os.makedirs(pathToModels + "/" + graphID)
    np.save(pathToModels + "/graphID/" + graphID + '-embeddings.npy', embeddings)

    with open(pathToModels + "/graphID/" + graphID + '-embeddings.json', 'w') as f:
        json.dump(nodeToIndex, f)

    with open(pathToModels + "/graphID/" + graphID + '-idxtoid.json', 'w') as f:
        json.dump(indexToNode, f)


if __name__ == "__main__":
    if len(sys.argv) == 6:
        predict(sys.argv[1],sys.argv[2],sys.argv[3],sys.argv[4],sys.argv[5])
    else:
        print("usage:  python predict.py <args>")