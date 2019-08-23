from __future__ import print_function

import numpy as np
import random
import json
import sys
import os

import tensorflow as tf
import networkx as nx
from networkx.readwrite import json_graph

version_info = list(map(int, nx.__version__.split('.')))
major = version_info[0]
minor = version_info[1]
assert (major <= 1) and (minor <= 11), "networkx major version > 1.11"

WALK_LEN = 5
N_WALKS = 50

flags = tf.app.flags
FLAGS = flags.FLAGS


def preprocess_data(prefix,attr_prefix,worker,isLabel=False,isFeatures=False,normalize=True,load_walks=False):

    np.random.seed(1)
    random.seed(1)
    G_local = nx.read_edgelist(prefix + '_' + worker, nodetype=int)
    G_central = nx.read_edgelist(prefix + '_centralstore_' + worker, nodetype=int)
    G = nx.compose(G_local, G_central)
    nodes_key_list = list(G.nodes())
    save_dir = FLAGS.base_log_dir + "/jasminegraph-local_trained_model_store/"
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)

    nodes_key_list = np.random.permutation(nodes_key_list)
    #node separation
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
    if isinstance(G.nodes()[0], int):
        conversion = lambda n: int(n)
    else:
        conversion = lambda n: n

    feat_data = None
    node_map = {}
    label_map = {}
    walks = []
    if(isFeatures):
    # TODO Try to get number of attri. from db
        with open(attr_prefix + "_attributes_" + worker) as fp:
            first_line = fp.readline().strip().split()

        num_nodes = len(nodes_key_list)
        if(isLabel):
            num_feats = len(first_line) - 2
        else:
            num_feats = len(first_line) - 1

        if(isLabel):
            labels = np.empty((num_nodes, 1), dtype=np.int64)

        feat_data = np.zeros((num_nodes, num_feats))
        with open(attr_prefix + "_attributes_" + worker) as fp:
            for i, line in enumerate(fp):
                info = line.strip().split()
                if(isLabel):
                    feat_data[i, :] = list(map(float, info[1:-1]))
                    node_map[info[0]] = i
                    if not info[-1] in label_map:
                        label_map[info[-1]] = len(label_map)
                    labels[i] = label_map[info[-1]]
                else:
                    feat_data[i, :] = list(map(float, info[1:]))
                    node_map[info[0]] = i
            next_index = i + 1

        with open(attr_prefix + "_centralstore_attributes_" + worker) as fp:
            for i, line in enumerate(fp):
                info = line.strip().split()
                if info[0] in node_map:
                    continue
                else:
                    if(isLabel):
                        feat_data[next_index, :] = list(map(float, info[1:-1]))
                        node_map[info[0]] = next_index
                        if not info[-1] in label_map:
                            label_map[info[-1]] = len(label_map)
                        labels[next_index] = label_map[info[-1]]
                    else:
                        feat_data[next_index, :] = list(map(float, info[1:]))
                        node_map[info[0]] = next_index
                    next_index += 1

    else:
        for i,node in  enumerate(G.nodes()):
            node_map[node] = i
    node_map = {conversion(k): int(v) for k, v in node_map.items()}

    if(isLabel):
        labels_reshape = labels.flatten()
        n_labels = len(np.unique(labels_reshape))
        labels_one_hot = np.eye(n_labels, dtype=int)[labels_reshape]
        class_map = {i: list(labels_one_hot[k]) for i, k in node_map.items()}

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
        if (G.node[edge[0]]['val'] or G.node[edge[1]]['val'] or
                G.node[edge[0]]['test'] or G.node[edge[1]]['test']):
            G[edge[0]][edge[1]]['train_removed'] = True
        else:
            G[edge[0]][edge[1]]['train_removed'] = False
        if(G.node[edge[0]]['val'] and G.node[edge[1]]['val']):
            G[edge[0]][edge[1]]['validation'] = True
        else:
            G[edge[0]][edge[1]]['validation'] = False
        if(G.node[edge[0]]['test'] and G.node[edge[1]]['test']):
            G[edge[0]][edge[1]]['testing'] = True
        else:
            G[edge[0]][edge[1]]['testing'] = False

    if normalize and not feat_data is None:
        # comes here only if normalize == true and the data set has feat
        from sklearn.preprocessing import StandardScaler
        train_ids = np.array([node_map[n] for n in G.nodes() if not G.node[n]['val'] and not G.node[n]['test']])
        train_feats = feat_data[train_ids]
        scaler = StandardScaler()
        scaler.fit(train_feats)
        feat_data = scaler.transform(feat_data)

    if(isLabel):
        return G,feat_data,node_map,walks,class_map,G_local
    else:
        return G, feat_data, node_map, walks, G_local

def load_data(prefix, worker,isLabel, normalize=True, load_walks=False):
    save_dir = FLAGS.base_log_dir + "/jasminegraph-local_trained_model_store/"
    G_data = json.load(open(save_dir + str(FLAGS.graph_id) + "_" + worker + "-G.json"))
    G = json_graph.node_link_graph(G_data)

    if isinstance(G.nodes()[0], int):
        conversion = lambda n: int(n)
    else:
        conversion = lambda n: n

    if os.path.exists(save_dir + str(FLAGS.graph_id) + "_" + worker + "-feats.npy"):
        feats = np.load(save_dir + str(FLAGS.graph_id) + "_" + worker + "-feats.npy")
    else:
        print("No features present.. Only identity features will be used.")
        feats = None
    id_map = json.load(open(save_dir + str(FLAGS.graph_id) + "_" + worker + "-id_map.json"))
    id_map = {conversion(k): int(v) for k, v in id_map.items()}
    # id_map doesn't have an order
    walks = []
    if(isLabel):
        class_map = json.load(open(save_dir + str(FLAGS.graph_id) + "_" + worker + "-class_map.json"))
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
        if (G.node[edge[0]]['val'] or G.node[edge[1]]['val'] or
                G.node[edge[0]]['test'] or G.node[edge[1]]['test']):
            G[edge[0]][edge[1]]['train_removed'] = True
        else:
            G[edge[0]][edge[1]]['train_removed'] = False
        if(G.node[edge[0]]['val'] and G.node[edge[1]]['val']):
            G[edge[0]][edge[1]]['validation'] = True
        else:
            G[edge[0]][edge[1]]['validation'] = False
        if(G.node[edge[0]]['test'] and G.node[edge[1]]['test']):
            G[edge[0]][edge[1]]['testing'] = True
        else:
            G[edge[0]][edge[1]]['testing'] = False

    if normalize and not feats is None:
        # comes here only if normalize == true and the data set has feat
        from sklearn.preprocessing import StandardScaler
        train_ids = np.array([id_map[n] for n in G.nodes() if not G.node[n]['val'] and not G.node[n]['test']])
        train_feats = feats[train_ids]
        scaler = StandardScaler()
        scaler.fit(train_feats)
        feats = scaler.transform(feats)

    if load_walks:
        with open(save_dir + str(FLAGS.graph_id) + "_" + worker + "-walks.txt") as fp:
            for line in fp:
                walks.append(map(conversion, line.split()))

    if(isLabel):
        return G, feats, id_map, walks, class_map
    else:
        return G, feats, id_map, walks


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
