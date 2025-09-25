from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError, ServiceUnavailable, CypherSyntaxError

class Neo4jClient:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def send_cypher_query(self, query: str) :
        try:
            with self.driver.session() as session:
                result = session.run(query)
                print(result)  # Optional: you may want to log this instead
                return [record.data() for record in result]
        except CypherSyntaxError as e:
            print(f"Cypher syntax error: {e}")
            return f"Cypher syntax error: {e}"
        except ServiceUnavailable as e:
            print(f"Database unavailable: {e}")
            return f"Database unavailable: {e}"
        except Neo4jError as e:
            print(f"Neo4j general error: {e}")
            return f"Neo4j general error: {e}"
        except Exception as e:
            print(f"Unexpected error: {e}")
            return f"Unexpected error: {e}"
