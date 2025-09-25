import socket
from time import sleep
import os
import json
import logging
from dotenv import load_dotenv

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
You are a Cypher query beginner assisting to answer a question.

Given the graph schema:
{schema}

Use only the following cypher query syntax template:
{template}

Generate sufficient minimal number of independent or dependent questions which can be converted to Cypher queries. Later we will answer the full question using these sub-answers.

Question: {question}

Return only the questions. No numbering. No extra explanations and sentences like "Here are the sub-questions:"
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

    def split_into_subquestions(self, question: str) -> list[str]:
        response = self.subquestion_chain.run(
            question=question.strip(),
            template=self.template,
            schema=self.schema.strip()
        )
        lines = response.strip().split("\n")
        # log lines for debugging
        logging.debug(f"Sub-questions generated: {response}")
        # Filter out empty lines and strip whitespace

        return [line.strip() for line in lines if line.strip()]

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

    def answer_question(self, question: str) -> str:
        cypher = self.query_chain(question)
        print(f"[LangChain Agent] Cypher Query:\n{cypher}\n")

        raw_result = self.send_cypher_query(cypher)
        try:
            result_data = json.loads(raw_result)
        except json.JSONDecodeError:
            result_data = raw_result

        return f"Answer:\n{result_data}"


if __name__ == "__main__":
    load_dotenv()
    agent = LangChainCypherAgent(api_key=os.getenv("OPENAI_API_KEY"))
    # question = input("Ask a graph question: ")
    question = "what are the terrorist attacks that occurred in germany?"
    answer = agent.answer_question(question)
    print(answer)
