import socket
from time import sleep
import os
import json
import logging
from dotenv import load_dotenv
from Neo4jClient import Neo4jClient
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
        self.client = Neo4jClient("bolt://localhost:7687", "neo4j", "")

        self.llm = Ollama(model="llama3")  # Replace with ChatOpenAI() if needed


        self.schema = """{[
  {
    "nodes": [
      {
        "identity": -102,
        "labels": [
          "Actor"
        ],
        "properties": {
          "name": "Actor",
          "indexes": [

          ],
          "constraints": [

          ]
        },
        "elementId": "-102"
      },
      {
        "identity": -103,
        "labels": [
          "Director"
        ],
        "properties": {
          "name": "Director",
          "indexes": [

          ],
          "constraints": [

          ]
        },
        "elementId": "-103"
      },
      {
        "identity": -101,
        "labels": [
          "Person"
        ],
        "properties": {
          "name": "Person",
          "indexes": [

          ],
          "constraints": [

          ]
        },
        "elementId": "-101"
      },
      {
        "identity": -100,
        "labels": [
          "Location"
        ],
        "properties": {
          "name": "Location",
          "indexes": [

          ],
          "constraints": [

          ]
        },
        "elementId": "-100"
      }
    ],
    "relationships": [
      {
        "identity": -111,
        "start": -101,
        "end": -101,
        "type": "FRIENDS",
        "properties": {
          "name": "FRIENDS"
        },
        "elementId": "-111",
        "startNodeElementId": "-101",
        "endNodeElementId": "-101"
      },
      {
        "identity": -109,
        "start": -100,
        "end": -101,
        "type": "FRIENDS",
        "properties": {
          "name": "FRIENDS"
        },
        "elementId": "-109",
        "startNodeElementId": "-100",
        "endNodeElementId": "-101"
      },
      {
        "identity": -110,
        "start": -101,
        "end": -100,
        "type": "FRIENDS",
        "properties": {
          "name": "FRIENDS"
        },
        "elementId": "-110",
        "startNodeElementId": "-101",
        "endNodeElementId": "-100"
      },
      {
        "identity": -108,
        "start": -100,
        "end": -100,
        "type": "FRIENDS",
        "properties": {
          "name": "FRIENDS"
        },
        "elementId": "-108",
        "startNodeElementId": "-100",
        "endNodeElementId": "-100"
      },
      {
        "identity": -100,
        "start": -100,
        "end": -100,
        "type": "VISITS",
        "properties": {
          "name": "VISITS"
        },
        "elementId": "-100",
        "startNodeElementId": "-100",
        "endNodeElementId": "-100"
      },
      {
        "identity": -101,
        "start": -100,
        "end": -101,
        "type": "VISITS",
        "properties": {
          "name": "VISITS"
        },
        "elementId": "-101",
        "startNodeElementId": "-100",
        "endNodeElementId": "-101"
      },
      {
        "identity": -102,
        "start": -101,
        "end": -100,
        "type": "VISITS",
        "properties": {
          "name": "VISITS"
        },
        "elementId": "-102",
        "startNodeElementId": "-101",
        "endNodeElementId": "-100"
      },
      {
        "identity": -103,
        "start": -101,
        "end": -101,
        "type": "VISITS",
        "properties": {
          "name": "VISITS"
        },
        "elementId": "-103",
        "startNodeElementId": "-101",
        "endNodeElementId": "-101"
      },
      {
        "identity": -112,
        "start": -100,
        "end": -100,
        "type": "WORKS_AT",
        "properties": {
          "name": "WORKS_AT"
        },
        "elementId": "-112",
        "startNodeElementId": "-100",
        "endNodeElementId": "-100"
      },
      {
        "identity": -113,
        "start": -100,
        "end": -101,
        "type": "WORKS_AT",
        "properties": {
          "name": "WORKS_AT"
        },
        "elementId": "-113",
        "startNodeElementId": "-100",
        "endNodeElementId": "-101"
      },
      {
        "identity": -115,
        "start": -101,
        "end": -101,
        "type": "WORKS_AT",
        "properties": {
          "name": "WORKS_AT"
        },
        "elementId": "-115",
        "startNodeElementId": "-101",
        "endNodeElementId": "-101"
      },
      {
        "identity": -114,
        "start": -101,
        "end": -100,
        "type": "WORKS_AT",
        "properties": {
          "name": "WORKS_AT"
        },
        "elementId": "-114",
        "startNodeElementId": "-101",
        "endNodeElementId": "-100"
      },
      {
        "identity": -116,
        "start": -100,
        "end": -100,
        "type": "MANAGES",
        "properties": {
          "name": "MANAGES"
        },
        "elementId": "-116",
        "startNodeElementId": "-100",
        "endNodeElementId": "-100"
      },
      {
        "identity": -117,
        "start": -100,
        "end": -101,
        "type": "MANAGES",
        "properties": {
          "name": "MANAGES"
        },
        "elementId": "-117",
        "startNodeElementId": "-100",
        "endNodeElementId": "-101"
      },
      {
        "identity": -119,
        "start": -101,
        "end": -101,
        "type": "MANAGES",
        "properties": {
          "name": "MANAGES"
        },
        "elementId": "-119",
        "startNodeElementId": "-101",
        "endNodeElementId": "-101"
      },
      {
        "identity": -118,
        "start": -101,
        "end": -100,
        "type": "MANAGES",
        "properties": {
          "name": "MANAGES"
        },
        "elementId": "-118",
        "startNodeElementId": "-101",
        "endNodeElementId": "-100"
      },
      {
        "identity": -106,
        "start": -101,
        "end": -100,
        "type": "NEIGHBORS",
        "properties": {
          "name": "NEIGHBORS"
        },
        "elementId": "-106",
        "startNodeElementId": "-101",
        "endNodeElementId": "-100"
      },
      {
        "identity": -104,
        "start": -100,
        "end": -100,
        "type": "NEIGHBORS",
        "properties": {
          "name": "NEIGHBORS"
        },
        "elementId": "-104",
        "startNodeElementId": "-100",
        "endNodeElementId": "-100"
      },
      {
        "identity": -105,
        "start": -100,
        "end": -101,
        "type": "NEIGHBORS",
        "properties": {
          "name": "NEIGHBORS"
        },
        "elementId": "-105",
        "startNodeElementId": "-100",
        "endNodeElementId": "-101"
      },
      {
        "identity": -107,
        "start": -101,
        "end": -101,
        "type": "NEIGHBORS",
        "properties": {
          "name": "NEIGHBORS"
        },
        "elementId": "-107",
        "startNodeElementId": "-101",
        "endNodeElementId": "-101"
      }
    ]
  }
]"""

        self.subquestion_prompt = PromptTemplate(
            input_variables=["question", "schema"],
            template="""
You are a Cypher query beginner assisting to answer a question.

Given the graph schema:
{schema}

Generate sufficient minimal number of independent or dependent questions which can be converted to Cypher queries. Later we will answer the full question using these sub-answers.

Question: {question}

Return only the questions. No numbering. No extra explanations and sentences like "Here are the sub-questions:"
"""
        )
        self.subquestion_chain = LLMChain(llm=self.llm, prompt=self.subquestion_prompt)

        # NEW: Cypher generation prompt that includes previous result context
        self.query_prompt = PromptTemplate(
            input_variables=["schema", "question"],
            template="""
You are an beginner in Cypher query generation.

Graph schema:
{schema}

Current sub-question:
{question}


Return only a single-line Cypher query. No comments or explanations. no sentences like "Here is the Cypher query:" or "The Cypher query is:"
"""
        )
        self.query_chain = LLMChain(llm=self.llm, prompt=self.query_prompt)

    def split_into_subquestions(self, question: str) -> list[str]:
        response = self.subquestion_chain.run(
            question=question.strip(),

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
            context=context.strip()
        ).strip()



    def answer_question(self, question: str) -> str:
        cypher = self.generate_cypher(question)
        print(f"[LangChain Agent] Cypher Query:\n{cypher}\n")

        raw_result = self.client.send_cypher_query(cypher)
        # try:
        #     result_data = json.loads(raw_result)
        # except json.JSONDecodeError:
        #     result_data = raw_result

        return f"Answer:\n{raw_result}"


if __name__ == "__main__":
    load_dotenv()
    agent = LangChainCypherAgent(api_key=os.getenv("OPENAI_API_KEY"))
    # question = input("Ask a graph question: ")
    question = "Is Alice freinds with Bob?"
    answer = agent.answer_question(question)
    print(answer)
