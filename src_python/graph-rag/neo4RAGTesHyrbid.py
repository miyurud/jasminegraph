from neo4j import GraphDatabase
from neo4j_graphrag.generation import GraphRAG
from neo4j_graphrag.retrievers import HybridRetriever
from neo4j_graphrag.llm import OllamaLLM  # Using OllamaLLM

# === CONFIG ===
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USERNAME = "neo4j"
NEO4J_PASSWORD = ""
OLLAMA_MODEL = "llama3"

# === Connect to Neo4j ===
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USERNAME, NEO4J_PASSWORD))

# === Extract Schema from Neo4j ===
def extract_schema(driver):
    schema = []
    query = "CALL db.schema.nodeTypeProperties()"
    with driver.session() as session:
        results = session.run(query)
        for record in results:
            label = record["nodeType"]
            prop = record["propertyName"]
            schema.append((label, prop))

    from collections import defaultdict
    label_props = defaultdict(set)
    for label, prop in schema:
        label_props[label].add(prop)

    schema_lines = []
    for label, props in label_props.items():
        prop_str = ", ".join(sorted(props))
        schema_lines.append(f"(:{label} {{{prop_str}}})")

    return "\n".join(schema_lines)

schema_text = extract_schema(driver)
print("\n=== EXTRACTED SCHEMA ===")
print(schema_text)

# === Instantiate Ollama LLM ===
llm = OllamaLLM(model_name=OLLAMA_MODEL)

# === Use HybridRetriever instead of Text2CypherRetriever ===
retriever = HybridRetriever(driver=driver)

# === Set up GraphRAG ===
rag = GraphRAG(retriever=retriever, llm=llm)

# === Run a Natural Language Query ===
query_text = "where is Radio City?"
response = rag.search(query_text=query_text)

# === Output the Answer ===
print("\n=== FINAL ANSWER ===")
print(response.answer)

# === Close Neo4j connection ===
driver.close()
