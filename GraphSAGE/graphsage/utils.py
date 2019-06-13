from __future__ import print_function

import numpy as np
import random
import json
import sys
import os

import networkx as nx
from networkx.readwrite import json_graph
from collections import defaultdict

version_info = list(map(int, nx.__version__.split('.')))
major = version_info[0]
minor = version_info[1]
assert (major <= 1) and (minor <= 11), "networkx major version > 1.11"

WALK_LEN = 5
N_WALKS = 50


def preprocess_data(prefix, worker):
    np.random.seed(1)
    random.seed(1)
    # G = nx.read_edgelist(prefix + '_' + worker, nodetype=int)
    G_local = nx.read_edgelist(prefix + '_' + worker, nodetype=int)
    G_central = nx.read_edgelist(prefix + '_centralstore_' + worker, nodetype = int)
    G = nx.compose(G_local,G_central)
    nodes_key_list = list(G.nodes())

    # print(nodes_key_list)
    # shuffle data
    nodes_key_list= np.random.permutation(nodes_key_list)
    # random.shuffle(nodes_key_list)
    test_val_frac = 0.2
    for i, n in enumerate(nodes_key_list):
        if i < len(nodes_key_list) * test_val_frac:
            G.node[n]["test"] = True
            G.node[n]["val"] = False
        if len(nodes_key_list) * test_val_frac <= i < 2 * len(nodes_key_list) * test_val_frac:
            G.node[n]["test"] = False
            G.node[n]["val"] = True
        if i >= 2 * len(nodes_key_list) * test_val_frac:
            G.node[n]["test"] = False
            G.node[n]["val"] = False

    data = json_graph.node_link_data(G)
    print(len(data['nodes']))

    with open(prefix + '_' + worker + "-G.json", mode="w") as f:
        f.write(json.dumps(data))

    # TODO Try to get number of attri. from db
    with open(prefix + "_attributes_" + worker) as fp:
        first_line = fp.readline().strip().split()

    num_nodes = len(nodes_key_list)
    num_feats = len(first_line) - 2
    print(num_feats)

    feat_data = np.zeros((num_nodes, num_feats))
    labels = np.empty((num_nodes, 1), dtype=np.int64)
    print(labels.shape)
    node_map = {}
    label_map = {}

    with open(prefix + "_attributes_" + worker) as fp:
        for i, line in enumerate(fp):
            # print(i)
            # print(line)
            info = line.strip().split()
            feat_data[i, :] = list(map(float, info[1:-1]))
            node_map[info[0]] = i
            # idx_node_map[i] = info[0]
            if not info[-1] in label_map:
                label_map[info[-1]] = len(label_map)
            labels[i] = label_map[info[-1]]
        next_index = i+1

    with open(prefix + "_centralstore_attributes_" + worker) as fp:
        for i, line in enumerate(fp):
            info = line.strip().split()
            if info[0] in node_map:
                continue
            else:
                feat_data[next_index, :] = list(map(float, info[1:-1]))
                node_map[info[0]] = next_index
                if not info[-1] in label_map:
                    label_map[info[-1]] = len(label_map)
                labels[next_index] = label_map[info[-1]]
                next_index +=1


    with open(prefix + "_" + worker + "-id_map.json", mode="w") as f:
        f.write(json.dumps(node_map))

    labels_reshape = labels.flatten()
    # print(labels_reshape)
    n_labels = len(np.unique(labels_reshape))
    # print(n_labels)
    labels_one_hot = np.eye(n_labels, dtype=int)[labels_reshape]
    # print(np.eye(n_labels, dtype=int).shape)
    # print("label_one_hot-----------------------------------------------------")
    # print(labels_one_hot)
    # class_map = {k: list(labels_one_hot[i]) for i, k in enumerate(node_map.keys())}
    class_map = {i: list(labels_one_hot[k]) for i, k in node_map.items()}
    # class_map = {}
    # for i in range(num_nodes):
    #     class_map[idx_node_map[i]] = list(labels_one_hot[i])

    # print(class_map)
    # print(list(class_map.values())[2])
    with open(prefix + "_" + worker + "-class_map.json", mode="w") as f:
        f.write(json.dumps(class_map, default=int))

    np.save(prefix + "_" + worker + '-feats.npy', feat_data)
    return load_data(prefix, worker)


def load_data(prefix, worker, normalize=True, load_walks=False):
    G_data = json.load(open(prefix +"_"+worker +"-G.json"))
    G = json_graph.node_link_graph(G_data)
    # print("G.nodes()[0] -----")
    # print(G.nodes()[0])
    if isinstance(G.nodes()[0], int):
        conversion = lambda n: int(n)
    else:
        conversion = lambda n: n

    if os.path.exists(prefix + "_" + worker + "-feats.npy"):
        feats = np.load(prefix + "_" + worker + "-feats.npy")
    else:
        print("No features present.. Only identity features will be used.")
        feats = None
    id_map = json.load(open(prefix + "_" + worker + "-id_map.json"))
    id_map = {conversion(k): int(v) for k, v in id_map.items()}
    # id_map doesn't have an order
    walks = []
    class_map = json.load(open(prefix + "_" + worker + "-class_map.json"))
    if isinstance(list(class_map.values())[0], list):
        lab_conversion = lambda n: n
    else:
        lab_conversion = lambda n: int(n)

    class_map = {conversion(k): lab_conversion(v) for k, v in class_map.items()}

    ## Remove all nodes that do not have val/test annotations
    ## (necessary because of networkx weirdness with the Reddit data)
    broken_count = 0
    for node in G.nodes():
        if not 'val' in G.node[node] or not 'test' in G.node[node]:
            G.remove_node(node)
            broken_count += 1
    print("Removed {:d} nodes that lacked proper annotations due to networkx versioning issues".format(broken_count))

    ## Make sure the graph has edge train_removed annotations
    ## (some datasets might already have this..)
    print("Loaded data.. now preprocessing..")
    for edge in G.edges():
        # print(edge)
        if (G.node[edge[0]]['val'] or G.node[edge[1]]['val'] or
                G.node[edge[0]]['test'] or G.node[edge[1]]['test']):
            G[edge[0]][edge[1]]['train_removed'] = True
        else:
            G[edge[0]][edge[1]]['train_removed'] = False

    if normalize and not feats is None:
        # comes here only if normalize == true and the data set has feat
        from sklearn.preprocessing import StandardScaler
        train_ids = np.array([id_map[n] for n in G.nodes() if not G.node[n]['val'] and not G.node[n]['test']])
        # print(train_ids)
        train_feats = feats[train_ids]
        # print(train_feats.shape) (1208,1433)
        scaler = StandardScaler()
        scaler.fit(train_feats)
        feats = scaler.transform(feats)
        # print(feats.shape)

    if load_walks:
        with open(prefix + "_" + worker + "-walks.txt") as fp:
            for line in fp:
                walks.append(map(conversion, line.split()))

    return G, feats, id_map, walks, class_map


def run_random_walks(G, nodes, num_walks=N_WALKS):
    pairs = []
    for count, node in enumerate(nodes):
        print(G.degree(node))
        if G.degree(node) == 0:
            continue
        for i in range(num_walks):
            curr_node = node
            for j in range(WALK_LEN):
                next_node = random.choice(G.neighbors(curr_node))
                # self co-occurrences are useless
                if curr_node != node:
                    pairs.append((node, curr_node))
                curr_node = next_node
        if count % 1000 == 0:
            print("Done walks for", count, "nodes")
    return pairs


if __name__ == "__main__":
    """ Run random walks """
    graph_file = sys.argv[1]
    out_file = sys.argv[2]
    G_data = json.load(open(graph_file))
    G = json_graph.node_link_graph(G_data)
    nodes = [n for n in G.nodes() if not G.node[n]["val"] and not G.node[n]["test"]]
    G = G.subgraph(nodes)
    pairs = run_random_walks(G, nodes)
    with open(out_file, "w") as fp:
        fp.write("\n".join([str(p[0]) + "\t" + str(p[1]) for p in pairs]))
