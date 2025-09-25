from langchain.vectorstores import FAISS
from langchain.embeddings import HuggingFaceEmbeddings
from neo4j import GraphDatabase
import numpy as np

# === Config ===
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = ""

# === Step 1: Connect to Neo4j ===
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

def fetch_nodes_with_properties(tx):
    query = "MATCH (n) RETURN id(n) AS id, properties(n) AS props LIMIT 10000"
    return [(record["id"], record["props"]) for record in tx.run(query)]

# === Step 2: Format Text for Embedding ===
def format_node_text(props: dict) -> str:
    return ", ".join([f"{k}: {v}" for k, v in props.items()])

# === Step 3: Fetch and Prepare Node Data ===
with driver.session() as session:
    raw_nodes = session.read_transaction(fetch_nodes_with_properties)

node_ids = []
node_texts = []

for node_id, props in raw_nodes:
    node_ids.append(node_id)
    node_texts.append(format_node_text(props))

# === Step 4: Embed with HuggingFaceEmbeddings ===
# === Step 4: Embed with HuggingFaceEmbeddings (trust custom code) ===
embedder = HuggingFaceEmbeddings(
    model_name="all-MiniLM-L6-v2"

)
embeddings = embedder.embed_documents(node_texts)
embedding_dim = len(embeddings[0])

# === Step 5: Create FAISS index ===
import faiss
index = faiss.IndexFlatL2(embedding_dim)
index.add(np.array(embeddings))

# === Step 6: Retrieve Similar Nodes ===
def retrieve_similar_nodes(query, top_k=5):
    query_vec = embedder.embed_query(query)
    D, I = index.search(np.array([query_vec]), top_k)
    return [(node_ids[i], node_texts[i], D[0][idx]) for idx, i in enumerate(I[0])]

# === Step 7: Multihop neighbors Cypher query ===
def fetch_multihop_neighbors_with_rels(tx, node_id):
    query = """
    MATCH path = (n)-[*1..2]-(m)
    WHERE id(n) = $node_id
    RETURN DISTINCT id(m) AS id, properties(m) AS props, relationships(path) AS rels, nodes(path) AS path_nodes
    """
    return [
        {
            "neighbor_id": record["id"],
            "neighbor_props": record["props"],
            "relationships": [dict(r) for r in record["rels"]],
            "path_nodes": [dict(node) for node in record["path_nodes"]],
        }
        for record in tx.run(query, node_id=node_id)
    ]

if __name__ == "__main__":
    print("Connected to Neo4j & FAISS index ready!")
    while True:
        query = "Who is Adam Clayton Powell Jr.?"
        if query.lower() == "exit":
            break

        results = retrieve_similar_nodes(query)
        print("\nTop Matches:")
        for node_id, text, dist in results:
            print(f"[{node_id}] {text} (distance: {dist:.4f})")

        explore = input("\nExplore multihop neighbors of these nodes? (y/n): ").strip().lower()
        if explore == "y":
            with driver.session() as session:
                for node_id, text, _ in results:
                    neighbors = session.read_transaction(fetch_multihop_neighbors_with_rels, node_id)
                    print(f"\nNeighbors of Node {node_id} ({text}):")
                    if neighbors:
                        for neighbor in neighbors:
                            n_id = neighbor["neighbor_id"]
                            n_props = neighbor["neighbor_props"]
                            rels = neighbor["relationships"]

                            n_text = format_node_text(n_props)
                            print(f"  - [{n_id}] {n_text}")

                            for rel in rels:
                                print(rel)
                                # r_type = rel.label
                                r_props = dict(rel)
                                print(f"     ↳ relationship: , properties: {r_props}")
                    else:
                        print("  - No neighbors found.")

