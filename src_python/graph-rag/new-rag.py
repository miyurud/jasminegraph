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
# Setup Ollama local API base and dummy key
openai.api_base = "http://localhost:11434/v1"  # Ollama server URL
openai.api_key = "ollama"  # dummy key for Ollama, must be non-empty

# Neo4j connection config
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASS = "your_password"

# Embedding model
embedding_model = HuggingFaceEmbeddings(model_name="all-MiniLM-L6-v2")

# Connect to Neo4j
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))

def get_graph_facts(limit=100000):
    query = """
    MATCH (a)-[r]->(b)
    RETURN a, type(r) as rel, b
    LIMIT $limit
    """
    facts = []
    with driver.session() as session:
        result = session.run(query, limit=limit)
        for record in result:
            a, b, rel = record["a"], record["b"], record["rel"]
            fact_text = f"{a.labels} {dict(a)} --[{rel}]--> {b.labels} {dict(b)}"
            facts.append(Document(page_content=fact_text))
    return facts

# Fetch and embed graph facts
docs = get_graph_facts()
vectorstore = FAISS.from_documents(docs, embedding_model)

retriever = vectorstore.as_retriever(search_kwargs={"k": 10})

# Use Ollama LLaMA 3 via ChatOpenAI wrapper
llm = ChatOpenAI(
    model="llama3",  # Ollama llama3 model name
    temperature=0,
    openai_api_base="http://localhost:11434/v1",  # override for Ollama
    openai_api_key="ollama"  # dummy key
)

qa_chain = RetrievalQA.from_chain_type(
    llm=llm,
    retriever=retriever,
    chain_type="stuff"
)
def show_retrieved_facts(question: str, k: int = 5):
    retrieved_docs = retriever.get_relevant_documents(question)
    f = ""
    print("\n🔍 Top Retrieved Facts:")
    for i, doc in enumerate(retrieved_docs, 1):
        print(f"\n--- Fact {i} ---\n{doc.page_content}")
        f += f"\n--- Fact {i} ---\n{doc.page_content}"
    return f

def answer_question(question: str):
    return qa_chain.run(question)



if __name__ == "__main__":
    question = "where is Radio City located?"
    retrieved_docs = show_retrieved_facts(question)

    load_dotenv()
    agent = LangChainCypherAgent(api_key=os.getenv("OPENAI_API_KEY"))
    anwer = agent.answer_question_with_multi_hop(question, retrieved_docs)
    answer = answer_question(question)
    print("\nAnswer:", answer)
    # answer = agent.answer_question(question, retrieved_docs)
    # print(answer)


