import networkx as nx
from pykeen.pipeline import pipeline
from sentence_transformers import SentenceTransformer
from transformers import pipeline as hf_pipeline
import numpy as np

# 1. Load Knowledge Graph from Neo4j (NO CYPHER — via bolt driver)
from neo4j import GraphDatabase


def load_graph_from_neo4j(uri, user, password):
    driver = GraphDatabase.driver(uri, auth=(user, password))
    G = nx.MultiDiGraph()

    with driver.session() as session:
        # Use low-level API to get data without Cypher queries
        nodes = session.run("MATCH (n) RETURN n")

        for record in nodes:
            node = record["n"]
            node_id = str(node.id)
            G.add_node(node_id, **dict(node.items()))

        rels = session.run("MATCH (a)-[r]->(b) RETURN a, r, b")
        for record in rels:
            src = str(record["a"].id)
            dst = str(record["b"].id)
            rel = record["r"]
            G.add_edge(src, dst, key=rel.type, **dict(rel.items()))

    return G


# 2. Embed the graph using TransE
def train_transe_embedding(graph):
    triples = []

    for u, v, k in graph.edges(keys=True):
        head = graph.nodes[u].get('name', u)
        tail = graph.nodes[v].get('name', v)
        rel = k
        triples.append((head, rel, tail))

    with open("triples.txt", "w") as f:
        for h, r, t in triples:
            f.write(f"{h}\t{r}\t{t}\n")

    result = pipeline(
        model='TransE',
        training={'path': 'triples.txt'},
    )

    return result.model


# 3. Embed user query and match against node embeddings
def embed_nodes(graph, embedder):
    node_embeddings = {}
    for nid, data in graph.nodes(data=True):
        text = data.get('name', '') + ' ' + data.get('description', '')
        embedding = embedder.encode(text)
        node_embeddings[nid] = embedding
    return node_embeddings


def retrieve_subgraph(query, embedder, node_embeddings, graph, top_k=5):
    query_embedding = embedder.encode(query)

    # Compute similarity
    scores = []
    for nid, emb in node_embeddings.items():
        sim = query_embedding @ emb / (np.linalg.norm(query_embedding) * np.linalg.norm(emb))
        scores.append((nid, sim))

    # Sort and retrieve top nodes
    top_nodes = sorted(scores, key=lambda x: x[1], reverse=True)[:top_k]
    top_ids = [nid for nid, _ in top_nodes]

    # Induce subgraph
    return graph.subgraph(top_ids).copy()


# 4. Answer using LLM
def subgraph_to_text(subgraph):
    facts = []
    for u, v, k, data in subgraph.edges(data=True, keys=True):
        head = subgraph.nodes[u].get('name', u)
        rel = k
        tail = subgraph.nodes[v].get('name', v)
        facts.append(f"{head} {rel} {tail}")
    return "\n".join(facts)


def answer_query_with_subgraph(query, subgraph):
    context = subgraph_to_text(subgraph)
    prompt = f"Context:\n{context}\n\nQuestion: {query}\nAnswer:"

    qa = hf_pipeline("text-generation", model="gpt2")
    result = qa(prompt, max_new_tokens=100)[0]['generated_text']
    return result


# 5. Full Pipeline
def graph_rag_pipeline(query):
    graph = load_graph_from_neo4j("bolt://localhost:7687", "neo4j", "password")
    embedder = SentenceTransformer("all-MiniLM-L6-v2")
    node_embeddings = embed_nodes(graph, embedder)
    subgraph = retrieve_subgraph(query, embedder, node_embeddings, graph)
    answer = answer_query_with_subgraph(query, subgraph)
    return answer


# --- Example usage
if __name__ == "__main__":
    query = "Who collaborated with Alice?"
    print(graph_rag_pipeline(query))
