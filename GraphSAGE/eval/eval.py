import tensorflow as tf
import numpy as np
import networkx as nx
import json
import random
from networkx.readwrite import json_graph

flags = tf.app.flags
FLAGS = flags.FLAGS

flags.DEFINE_string('graph_id','3', 'specify the graphID')
flags.DEFINE_string('path', '/var/tmp/jasminegraph-localstore/jasminegraph-local_trained_model_store/3', 'name of the object file that stores the training data. must be specified.')
flags.DEFINE_integer('max_degree', 100, 'maximum node degree.')
flags.DEFINE_integer('neg_sample_size', 20, 'number of negative samples')

def construct_deg(G,id2idx):
    adj = len(id2idx)*np.ones((len(id2idx)+1, FLAGS.max_degree))
    deg = np.zeros((len(id2idx),))
    for nodeid in G.nodes():
        neighbors = np.array([id2idx[str(neighbor)] for neighbor in G.neighbors(nodeid)])
        deg[id2idx[str(nodeid)]] = len(neighbors)
        if len(neighbors) == 0:
            continue
        if len(neighbors) > FLAGS.max_degree:
            neighbors = np.random.choice(neighbors, FLAGS.max_degree, replace=False)
        elif len(neighbors) < FLAGS.max_degree:
            neighbors = np.random.choice(neighbors, FLAGS.max_degree, replace=True)
        adj[id2idx[str(nodeid)], :] = neighbors
    return adj, deg

def batch_feed_dict(batch_edges,id2idx):
    batch1 = []
    batch2 = []
    for node1, node2 in batch_edges:
        batch1.append(id2idx[str(node1)])
        batch2.append(id2idx[str(node2)])

    return batch1,batch2

def test_feed_dict(edge_list,id2idx,size=None):

    if size is None:
        return batch_feed_dict(edge_list,id2idx)
    else:
        ind = np.random.permutation(len(edge_list))
        test_edges = [edge_list[i] for i in ind[:len(ind)]]
        return batch_feed_dict(test_edges,id2idx)

#get test edge files
def eval(graph,test_edges,node_idx_map,idx_node_map,graph_embeddings):
    adj, deg = construct_deg(graph,node_idx_map)

    batch1,batch2=test_feed_dict(test_edges,node_idx_map)
    neg_samples = []
    for i in range(len(batch1)):
        neg =[]
        while(len(neg)<FLAGS.neg_sample_size):
            x = random.randint(0,len(adj)-2)
            if(graph.has_edge(idx_node_map[str(batch1[i])],idx_node_map[str(x)])==False and x not in neg):
                neg.append(x)
        neg_samples.append(neg)

    output1 =[]
    output2 =[]
    neg_out = []
    for i in range(len(batch1)):
        output1.append(graph_embeddings[batch1[i]])
    for j in range(len(batch2)):
        output2.append(graph_embeddings[batch2[j]])
    for k in range(len(neg_samples)):
        neg_out_part =[]
        for m in range(len(neg_samples[k])):
            neg_out_part.append(graph_embeddings[neg_samples[k][m]])
        neg_out.append(neg_out_part)

    aff = np.sum(np.array(output1)*np.array(output2),axis=1)
    neg_aff = [np.sum(np.array(output1[i])*neg_out[i],axis=1) for i in range(len(output1))]

    hit_1 = 0
    hit_3 = 0
    hit_10 = 0
    hit_50 = 0
    mrr = 0
    for x in range(len(aff)):
        list = np.append(neg_aff[x],aff[x]).tolist()
        list.sort()
        list= list[::-1]
        rank =(list.index(aff[x])+1)
        reciprocal = 1/rank
        mrr+=reciprocal
        if(rank==1):
            hit_1+=1
        if(rank<=3):
            hit_3+=1
        if(rank<=10):
            hit_10+=1
        if(rank<=50):
            hit_50+=1
    mrr = mrr/(len(aff))
    hit_1 = hit_1/(len(aff))
    hit_3 = hit_3/(len(aff))
    hit_10 = hit_10/(len(aff))
    hit_50 = hit_50/(len(aff))
    print("MRR = "+ str(mrr))
    print("hit@1 = "+ str(hit_1))
    print("hit@3 = "+ str(hit_3))
    print("hit@10 = "+ str(hit_10))
    print("hit@50 = "+ str(hit_50))

def load(path,graphID):
    G = nx.read_edgelist(path+"/" +graphID, nodetype=int)
    #test edge list
    test_edges_graph = nx.read_edgelist(path +"/"+graphID+"_TEST_EDGE_SET.txt", nodetype=int)
    test_edges = [e for e in test_edges_graph.edges()]
    #load embeddings
    node_idx_map = json.load(open(path +"/" +graphID +'-embeddings.json'))
    idx_node_map = json.load(open(path+ "/" + graphID + '-idxtoid.json'))
    graph_embeddings = np.load(path + "/" + graphID + '-embeddings.npy')
    return G,test_edges,node_idx_map,idx_node_map,graph_embeddings;

def main(argv=None):
    print("Loading training data..")
    graph,test_edges,node_idx_map,idx_node_map,graph_embeddings = load(FLAGS.path,FLAGS.graph_id)
    print("Done loading training data..")
    eval(graph,test_edges,node_idx_map,idx_node_map,graph_embeddings)


if __name__ == '__main__':
    main()
    # python -m eval
