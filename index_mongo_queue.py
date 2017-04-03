#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys, re
from lru import split_lru_in_stems, clean_lru
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

def lru_to_stemnodes(lru):
    stems = []
    lru = clean_lru(lru)
    sublru = ""
    for typ, val, stem in split_lru_in_stems(lru):
        sublru += stem + "|"
        stems.append({
            "LRU": sublru,
            "type": typ,
            "stem": stem,
            "page": False
        })
    stems[-1]["page"] = True
    return stems

def lru_to_stemnodes_bigrams(lru):
    stems = lru_to_stemnodes(lru)
    return zip(stems, stems[1:])

def run_load(session):
    pass


if __name__ == "__main__":
    try:
        from config import neo4j_host, neo4j_port, neo4j_user, neo4j_pass
    except:
        sys.stderr.write("ERROR: please create & fill config.py from config.py.example first")
        exit(1)
    neo4jdriver = GraphDatabase.driver("bolt://%s:%s" % (neo4j_host, neo4j_port), auth=(neo4j_user, neo4j_pass))
    with neo4jdriver.session() as session:
        run_load(session)
    print(lru_to_stemnodes_bigrams("s:http|h:fr|h:sciences-po|h:medialab|"))
