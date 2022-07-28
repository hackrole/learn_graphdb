import time
from neo4j import GraphDatabase

uri = "neo4j://localhost:7687"
user = "neo4j"
pwd = "test"


driver = GraphDatabase.driver(uri, auth=(user, pwd))

def run_query(tx):
    query = "match (c:Case) return c limit 10"
    rt = tx.run(query)
    return rt

s = time.time()
with driver.session() as session:
    session.read_transaction(run_query)
print(f"send time: {time.time() - s}")

