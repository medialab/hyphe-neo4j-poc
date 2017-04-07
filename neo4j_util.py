#!/usr/bin/env python
# -*- coding: utf-8 -*-

from neo4j.v1 import GraphDatabase
from warnings import filterwarnings
filterwarnings(action='ignore', category=UserWarning,
               message="Bolt over TLS is only available")

class Neo4J(object):

    def __init__(self, config={}, queries_file="queries/core.cypher"):
        if not config:
            config = {
              "host": "localhost",
              "port": 7687,
              "user": "neo4j",
              "pass": "neo4j"
            }
        self.config = config
        self.driver = GraphDatabase.driver(
          "bolt://%s:%s" % (config["host"], config["port"]),
          auth=(config["user"], config["pass"])
        )
        self.queries_file = queries_file
        self.queries = get_queries(queries_file)

    def query(self, query, mode, **kwargs):
        if mode not in ["read", "write"]:
            sys.stderr.write("ERROR: transaction query mode should be read "
                "or write\n")
            return None

        try:
            q = self.queries[query]
        except AttributeError:
            sys.stderr.write("ERROR: %s does not exist in %s\n" %
              (query, self.queries_file))
            return None

        with self.driver.session() as s:
            txquery = getattr(s, "%s_transaction" % mode)
            return txquery(lambda tx: tx.run(q, **kwargs))

    def write_query(self, query, **kwargs):
        return self.query(query, "write", **kwargs)

    def read_query(self, query, **kwargs):
        return self.query(query, "read", **kwargs)

def read_queries_file(queries_file_pointer):
    queries = {}
    name = ""
    for line in queries_file_pointer.read().split("\n"):
        if not line.strip():
            continue
        elif line.startswith("// name:"):
            name = line.replace("// name: ", "").strip()
            queries[name] = ""
        elif line.startswith("//"):
            continue
        elif queries[name]:
            queries[name] += "\n" + line
        else:
            queries[name] = line
    return queries

def get_queries(queries_file):
    with open(queries_file) as f:
        return read_queries_file(f)

