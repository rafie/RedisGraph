import os
import sys
import unittest
from redisgraph import Graph, Node, Edge

# import redis
from .disposableredis import DisposableRedis

from base import FlowTestsBase

redis_con = None
GRAPH_NAME = "G"
redis_graph = None
people = ["Roi", "Alon", "Ailon", "Boaz", "Tal", "Omri", "Ori"]
countries = ["Israel", "USA", "Japan", "United Kingdom"]
visits = [("Roi", "USA"), ("Alon", "Israel"), ("Ailon", "Japan"), ("Boaz", "United Kingdom")]

def redis():
    return DisposableRedis(loadmodule=os.path.dirname(os.path.abspath(__file__)) + '/../../src/redisgraph.so')

class GraphPersistency(FlowTestsBase):
    @classmethod
    def setUpClass(cls):
        print "GraphPersistency"
        global redis_graph
        global redis_con
        cls.r = redis()
        cls.r.start()
        redis_con = cls.r.client()
        redis_graph = Graph(GRAPH_NAME, redis_con)

        # redis_con = redis.Redis()
        # redis_graph = Graph("G", redis_con)

        cls.populate_graph()

    @classmethod
    def tearDownClass(cls):
        cls.r.stop()
        # pass

    @classmethod
    def populate_graph(cls):
        global redis_graph

        if not redis_con.exists(GRAPH_NAME):
            personNodes = {}
            countryNodes = {}
            # Create entities
            
            for p in people:
                person = Node(label="person", properties={"name": p})
                redis_graph.add_node(person)
                personNodes[p] = person
            
            for p in countries:
                country = Node(label="country", properties={"name": p})
                redis_graph.add_node(country)
                countryNodes[p] = country
            
            for v in visits:
                person = v[0]
                country = v[1]
                edge = Edge(personNodes[person], 'visit', countryNodes[country], properties={'purpose': 'pleasure'})
                redis_graph.add_edge(edge)

            redis_graph.commit()

            # Delete nodes, to introduce deleted item within our datablock
            query = """MATCH (n:person) WHERE n.name = 'Roi' or n.name = 'Ailon' DELETE n"""
            redis_graph.query(query)

            query = """MATCH (n:country) WHERE n.name = 'USA' DELETE n"""
            redis_graph.query(query)

            # Create index.
            actual_result = redis_con.execute_command("GRAPH.QUERY", GRAPH_NAME, "CREATE INDEX ON :person(name)")
            actual_result = redis_con.execute_command("GRAPH.QUERY", GRAPH_NAME, "CREATE INDEX ON :country(name)")

    # Connect a single node to all other nodes.
    def test01_save_load_rdb(self):
        for i in range(2):
            if i == 1:
                # Save RDB & Load from RDB
                redis_con.execute_command("DEBUG", "RELOAD")

            # Verify
            # Expecting 5 person entities.
            query = """MATCH (p:person) RETURN COUNT(p)"""
            actual_result = redis_graph.query(query)
            nodeCount = int(float(actual_result.result_set[1][0]))
            assert(nodeCount == 5)

            query = """MATCH (p:person) WHERE p.name='Alon' RETURN COUNT(p)"""
            actual_result = redis_graph.query(query)
            nodeCount = int(float(actual_result.result_set[1][0]))
            assert(nodeCount == 1)

            # Expecting 3 country entities.
            query = """MATCH (c:country) RETURN COUNT(c)"""
            actual_result = redis_graph.query(query)
            nodeCount = int(float(actual_result.result_set[1][0]))
            assert(nodeCount == 3)

            query = """MATCH (c:country) WHERE c.name = 'Israel' RETURN COUNT(c)"""
            actual_result = redis_graph.query(query)
            nodeCount = int(float(actual_result.result_set[1][0]))
            assert(nodeCount == 1)

            # Expecting 2 visit edges.
            query = """MATCH (n:person)-[e:visit]->(c:country) WHERE e.purpose='pleasure' RETURN COUNT(e)"""
            actual_result = redis_graph.query(query)        
            edgeCount = int(float(actual_result.result_set[1][0]))
            assert(edgeCount == 2)

            # Verify indices exists.
            actual_result = redis_con.execute_command("GRAPH.EXPLAIN", "G", "match (n:person) where n.name = 'Roi' RETURN n")
            assert("Index Scan" in actual_result)

            actual_result = redis_con.execute_command("GRAPH.EXPLAIN", "G", "match (n:country) where n.name = 'Israel' RETURN n")
            assert("Index Scan" in actual_result)

if __name__ == '__main__':
    unittest.main()