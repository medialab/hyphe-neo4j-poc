#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys, re
from lru import split_lru_in_stems, clean_lru
from read_queries import read_queries_file
from neo4j.v1 import GraphDatabase
from warnings import filterwarnings
filterwarnings(action='ignore', category=UserWarning, message="Bolt over TLS is only available")

# STEM NODE:
# - LRU: string
# - type: enum(s,h,t,p,q,f)
# - stem:
# - page: boolean
# - page_timestamp: int
# - page_http_code: integer
# - page_encoding: string
# - page_error: string
# - page_crawl_depth: integer
# - page_sources: enum(crawl,link,both) =>
types = {
  "s": "Scheme",
  "h": "Host",
  "t": "Port",
  "p": "Path",
  "q": "Query",
  "f": "Fragment"
}
def lru_to_stemnodes(lru):
    stems = []
    lru = clean_lru(lru)
    sublru = ""
    for typ, val, stem in split_lru_in_stems(lru):
        sublru += stem + "|"
        stems.append({
          "lru": sublru,
          "type": types[typ],
          "stem": stem,
          "page": False
        })
    stems[-1]["page"] = True
    return stems

def lru_to_stemnodes_bigrams(lru):
    stems = lru_to_stemnodes(lru)
    return zip(stems, stems[1:])

def write_query(session, query, **kwargs):
    return session.write_transaction(lambda tx: tx.run(query, **kwargs))

def read_query(session, query, **kwargs):
    return session.read_transaction(lambda tx: tx.run(query, **kwargs))

def init_neo4j(session, queries):
    write_query(session, queries["constrain_lru"])
    write_query(session, queries["startup"])

def run_load(session, queries):
    lru = "s:https|h:fr|h:sciencespo|h:medialab|p:tonpere|q:enslip"
    a = write_query(session, queries["index"], lrus=[lru_to_stemnodes(lru)])
    print(a._summary.counters.__dict__)


if __name__ == "__main__":
    try:
        from config import neo4j_host, neo4j_port, neo4j_user, neo4j_pass
    except:
        sys.stderr.write("ERROR: please create & fill config.py from config.py.example first")
        exit(1)

    with open("queries/notes.cypher") as f:
        queries = read_queries_file(f)

    neo4jdriver = GraphDatabase.driver("bolt://%s:%s" % (neo4j_host, neo4j_port), auth=(neo4j_user, neo4j_pass))
    with neo4jdriver.session() as session:
        init_neo4j(session, queries)
        run_load(session, queries)

