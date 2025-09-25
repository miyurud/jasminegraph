import os
import openai
from dotenv import load_dotenv
from neo4j import GraphDatabase
from langchain_core.documents import Document
from langchain_community.vectorstores import FAISS
from langchain.embeddings import HuggingFaceEmbeddings
from langchain.chat_models import ChatOpenAI
from langchain.chains import RetrievalQA
from CypherAgent import LangChainCypherAgent
import networkx as nx
# from node2vec import Node2Vec
from sklearn.metrics.pairwise import cosine_similarity

# Setup OpenAI/Ollama
openai.api_base = "http://localhost:11434/v1"
openai.api_key = "ollama"  # dummy key

# Neo4j config
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASS = "your_password"
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))

# Embedding model
embedding_model = HuggingFaceEmbeddings(model_name="all-MiniLM-L6-v2")

# -------------------- GRAPH UTILS --------------------

def fetch_graph_edges():
    query = "MATCH (a)-[r]->(b) RETURN id(a) as src, id(b) as dst"
    edges = []
    with driver.session() as session:
        result = session.run(query)
        for record in result:
            edges.append((record["src"], record["dst"]))
    return edges

def fetch_node_data():
    query = "MATCH (n) RETURN id(n) as id, labels(n) as labels, properties(n) as props"
    node_map = {}
    with driver.session() as session:
        result = session.run(query)
        for record in result:
            node_map[record["id"]] = {
                "labels": record["labels"],
                "props": record["props"]
            }
    return node_map

# def compute_node_embeddings(graph_edges, dimensions=64):
#     G = nx.DiGraph()
#     G.add_edges_from(graph_edges)
#     node2vec = Node2Vec(G, dimensions=dimensions, walk_length=10, num_walks=50, workers=2)
#     model = node2vec.fit(window=5, min_count=1)
#     return G, model

# -------------------- PPR & RETRIEVAL --------------------

def find_top_k_seed_nodes(question: str, node_info, embedder, k=5):
    node_texts = []
    id_to_index = []
    for node_id, data in node_info.items():
        label_text = " ".join(data["labels"])
        prop_text = " ".join(f"{k}: {v}" for k, v in data["props"].items())
        combined = f"{label_text} {prop_text}"
        node_texts.append(combined)
        id_to_index.append(node_id)

    node_embeddings = embedder.embed_documents(node_texts)
    question_vec = embedder.embed_query(question)

    sims = cosine_similarity([question_vec], node_embeddings)[0]
    top_indices = sims.argsort()[-k:][::-1]
    return [(id_to_index[i], sims[i]) for i in top_indices]

def personalized_page_rank_multi_seed(graph, seed_scores, alpha=0.85):
    personalization = {node_id: score for node_id, score in seed_scores}
    total = sum(personalization.values())
    for node_id in personalization:
        personalization[node_id] /= total
    return nx.pagerank(graph, alpha=alpha, personalization=personalization)

def get_ppr_docs(top_nodes, node_info, top_k=10):
    sorted_nodes = sorted(top_nodes.items(), key=lambda x: x[1], reverse=True)[:top_k]
    docs = []
    for node_id, score in sorted_nodes:
        data = node_info.get(node_id, {})
        text = f"{data['labels']} {data['props']} (PPR Score: {score:.4f})"
        docs.append(Document(page_content=text))
    return docs

# -------------------- LangChain Setup --------------------

llm = ChatOpenAI(
    model="llama3",
    temperature=0,
    openai_api_base="http://localhost:11434/v1",
    openai_api_key="ollama"
)

def build_qa_chain(docs):
    vectorstore = FAISS.from_documents(docs, embedding_model)
    retriever = vectorstore.as_retriever(search_kwargs={"k": 5})
    return RetrievalQA.from_chain_type(llm=llm, retriever=retriever, chain_type="stuff"), retriever

def show_retrieved_facts(retriever, question: str, k: int = 5):
    retrieved = retriever.get_relevant_documents(question)
    print("\n🔍 Top Retrieved PPR Facts:")
    for i, doc in enumerate(retrieved, 1):
        print(f"\n--- Fact {i} ---\n{doc.page_content}")
    return retrieved

# -------------------- MAIN --------------------

if __name__ == "__main__":
    load_dotenv()

    question = "Where was Radio City found?"

    # Step 1: Load graph structure
    edges = fetch_graph_edges()
    node_info = fetch_node_data()
    graph = nx.DiGraph()
    graph.add_edges_from(edges)
    # graph, node2vec_model = compute_node_embeddings(edges)

    # Step 2: Find seed nodes
    top_k_seeds = find_top_k_seed_nodes(question, node_info, embedding_model, k=5)

    # Step 3: Personalized PageRank
    ppr_scores = personalized_page_rank_multi_seed(graph, top_k_seeds)

    # Step 4: Create docs for top nodes
    docs = get_ppr_docs(ppr_scores, node_info)

    # Step 5: Retrieve and answer
    qa_chain, retriever = build_qa_chain(docs)
    show_retrieved_facts(retriever, question)
    print("\n✅ Answer:", qa_chain.run(question))

    # Optional: Cypher Agent Answer
    # agent = LangChainCypherAgent(api_key=os.getenv("OPENAI_API_KEY"))
    # cypher_answer = agent.answer_question(question, docs)
    # print("\n🤖 Cypher Agent Answer:", cypher_answer)
