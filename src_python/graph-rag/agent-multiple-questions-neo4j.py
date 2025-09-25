import socket
from time import sleep
import os
import json
import logging
from dotenv import load_dotenv
from Neo4jClient import Neo4jClient  # Ensure this is the correct import path for your Neo4j client
from langchain.chains.llm import LLMChain
from langchain_core.prompts import PromptTemplate
from langchain_community.llms import Ollama  # or replace with ChatOpenAI if needed

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")

class LangChainCypherAgent:
    def __init__(self, api_key=None):
        self.HOST = "127.0.0.1"
        self.PORT = 7777
        self.GRAPH_ID = "14"
        self.LINE_END = b"\r\n"
        self.CYPHER = b"cypher"

        self.llm = Ollama(model="llama3")  # Replace with ChatOpenAI() if needed
        self.client = Neo4jClient("bolt://localhost:7687", "neo4j", "")


        self.template = """
MATCH (n:Label) RETURN n.name
MATCH (n:Label) RETURN count(n)
MATCH (n:Label) RETURN n.${prop}
MATCH (n:Label {key: 'value', key2: 'value2'}) RETURN n
MATCH (n:Label {key: 'value', key2: 'value2'}) RETURN n.${prop}
MATCH (n:Label {key: 'value', key2: 'value2'}) RETURN n.name, n.${prop}
MATCH (n:Label) WHERE n.${prop1} = '${value1}' AND n.${prop2} = '${value2}' RETURN n.name
MATCH (n:Label {key: 'value', key2: 'value2'}) WHERE n.${prop1} = '${value1}' AND n.${prop2} = '${value2}' RETURN n.name
MATCH (n:Label) RETURN n.${prop}
MATCH (n:Label) WHERE n.${prop1} = '${value1}' OR n.${prop2} = '${value2}' RETURN n.name
MATCH (n:Label {key: 'value', key2: 'value2'}) WHERE n.${prop1} = '${value1}' OR n.${prop2} = '${value2}' RETURN n.name
MATCH (n:Label) RETURN ${agg_clause}
MATCH (n:Label) RETURN n.${key} AS key, count(n) AS num
MATCH (n:Label) RETURN n.${key} AS key, avg(n.${key}) AS avg
MATCH (n:Label)-[r0]->(m0:Label) RETURN count(m0)
MATCH (n:Label)-[r0]->(m0:Label) RETURN n, count(m0) AS num
MATCH (n:Label)-[r0]->(m0:Label) WHERE n.${prop1} = '${value1}' AND m0.${prop2} = '${value2}' RETURN n, m0
MATCH (n:Label)<-[r0]-(m0:Label) WHERE n.${prop1} = '${value1}' AND m0.${prop2} = '${value2}' RETURN n, m0
MATCH (n:Label)-[r0]->(m0:Label {key: 'value', key2: 'value2'}) RETURN n, m0
MATCH (n:Label)<-[r0]-(m0:Label {key: 'value', key2: 'value2'}) RETURN n, m0
MATCH (n:Label)-[r0]->(m0:Label)-[r1]->(m1:Label) WHERE n.${prop1} = '${value1}' AND m0.${prop2} = '${value2}' AND m1.${prop3} = '${value3}' RETURN n, m0, m1
        """

        self.schema = """{
"name": "terrorist_attack",
"entities": [
  {
    "label": "TerroristAttack",
    "properties": {
      "number_of_injuries": "int",
      "number_of_deaths": "int",
      "date": "date",
      "locations": "list[str]"
    }
  },
  {
    "label": "Terrorist",
    "properties": {
      "country_of_citizenship": "list[str]",
      "place_of_birth": "str",
      "gender": "str",
      "date_of_birth": "date"
    }
  },
  {
    "label": "Target",
    "properties": {}
  },
  {
    "label": "Country",
    "properties": {}
  },
  {
    "label": "Weapon",
    "properties": {}
  }
],
"relations": [
  {
    "label": "perpetratedBy",
    "subj_label": "TerroristAttack",
    "obj_label": "Terrorist"
  },
  {
    "label": "occursIn",
    "subj_label": "TerroristAttack",
    "obj_label": "Country"
  },
  {
    "label": "employs",
    "subj_label": "TerroristAttack",
    "obj_label": "Weapon"
  },
  {
    "label": "targets",
    "subj_label": "TerroristAttack",
    "obj_label": "Target"
  }
]
}"""
        self.subquestion_prompt = PromptTemplate(
            input_variables=["question", "schema", "template"],
            template="""
        You are an assistant generating Cypher queries to answer a complex question.

        Given the graph schema:
        {schema}

        And the Cypher query syntax template:
        {template}

        Break down the original question into the minimum number of atomic sub-questions that can be directly answered with simple Cypher queries.

        Return the output in the following JSON format:

        [
          {{
            "subquestion": "<sub-question>",
            "cypher": "<cypher query>"
          }},
          ...
        ]

        Only output a valid JSON array. Do not add any extra text.
        Question: {question}
        """
        )

        self.subquestion_chain = LLMChain(llm=self.llm, prompt=self.subquestion_prompt)

        # NEW: Cypher generation prompt that includes previous result context
        self.query_prompt = PromptTemplate(
            input_variables=["schema", "question", "template", "context"],
            template="""
You are an beginner in Cypher query generation.

Graph schema:
{schema}

Current sub-question:
{question}


Generate a valid clean and simple Cypher query using only this syntax template, using any other syntax will result in an error:
{template}

Return only a single-line Cypher query. No comments or explanations. no sentences like "Here is the Cypher query:" or "The Cypher query is:"
"""
        )
        self.query_chain = LLMChain(llm=self.llm, prompt=self.query_prompt)

    def split_into_subquestions(self, question: str) -> list[dict]:
        response = self.subquestion_chain.run(
            question=question.strip(),
            template=self.template,
            schema=self.schema.strip()
        )
        try:
            print(response)
            subq_pairs = json.loads(response)
            return subq_pairs
        except json.JSONDecodeError:
            logging.error("Failed to parse subquestions and cypher queries from LLM output")
            return []

    def generate_cypher(self, subquestion: str, context: str = "") -> str:
        return self.query_chain.run(
            schema=self.schema.strip(),
            question=subquestion.strip(),
            context=context.strip(),
            template=self.template.strip()
        ).strip()

    def send_cypher_query(self, query: str) -> str:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((self.HOST, self.PORT))
            sock.sendall(self.CYPHER + self.LINE_END)
            sleep(0.1)
            sock.recv(1024)
            sock.sendall(self.GRAPH_ID.encode() + self.LINE_END)
            sock.recv(1024)
            sock.sendall(query.encode() + self.LINE_END)

            response = b""
            while True:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                response += chunk
                if b"done\r" in response:
                    break
        return response

    def summarize_answer(self, all_answers: list[str], original_question: str) -> str:
        # Combine all sub-question answers and generate a human-friendly summary
        combined_context = "\n".join(all_answers)

        summary_prompt = PromptTemplate(
            input_variables=["original_question", "combined_context"],
            template="""
    You are a helpful assistant summarizing information from a graph database.

    Original question: {original_question}

    Here are the sub-questions, their Cypher queries, and raw answers:
    {combined_context}

    Now, based on this information, provide a clear and concise final answer in simple English that a non-technical user can understand. Do not mention Cypher or sub-questions. Just provide the summarized answer to the original question.
    """
        )
        summary_chain = LLMChain(llm=self.llm, prompt=summary_prompt)
        final_answer = summary_chain.run(original_question=original_question, combined_context=combined_context)
        return final_answer.strip()

    def answer_question(self, question: str) -> str:
        subq_pairs = self.split_into_subquestions(question)
        all_answers = []
        prior_result_text = ""

        for idx, item in enumerate(subq_pairs):
            subq = item.get("subquestion")
            cypher = item.get("cypher")
            print(f"[{idx + 1}] Sub-question: {subq}")
            print(f"[{idx + 1}] Cypher: {cypher}")

            try:
                raw_result = self.client.send_cypher_query(cypher)
                try:
                    result_data = json.loads(raw_result)
                except json.JSONDecodeError:
                    result_data = raw_result.decode(errors="ignore")

                prior_result_text = json.dumps(result_data)[:3000]  # optional if you plan to use it in summary
                all_answers.append(f"Q{idx + 1}: {subq}\nCypher: {cypher}\nA{idx + 1}: {result_data}\n")

            except Exception as e:
                all_answers.append(f"Q{idx + 1}: {subq}\nCypher: {cypher}\nA{idx + 1}: Failed ({str(e)})\n")

        summary = self.summarize_answer(all_answers, question)
        return summary


if __name__ == "__main__":
    load_dotenv()
    agent = LangChainCypherAgent(api_key=os.getenv("OPENAI_API_KEY"))
    # question = input("Ask a graph question: ")
    question = "What are the names and injury counts of terrorist attacks that targeted the same target as the 1993 World Trade Center bombing?"
    answer = agent.answer_question(question)
    print(answer)
