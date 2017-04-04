#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys, re
from lru import split_lru_in_stems, get_alternative_prefixes, name_lru, clean_lru
from read_queries import read_queries_file
from pymongo import MongoClient
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
          "stem": val
        })
    stems[-1]["page"] = True
    return stems

def prepare_lrus(lru, lruLinks, crawlMetas={}):
    lrus = []

    lrus.append(lru_to_stemnodes(lru))
    lrus[-1][-1]["crawled"] = True
    lrus[-1][-1]["crawlDepth"] = crawlMetas["depth"]
    lrus[-1][-1]["crawlTimestamp"] = crawlMetas["timestamp"]
    lrus[-1][-1]["crawlHTTPCode"] = crawlMetas["status"]
    lrus[-1][-1]["crawlError"] = crawlMetas["error"]
    lrus[-1][-1]["pageEncoding"] = crawlMetas["encoding"]

    for link in lruLinks:
        lrus.append(lru_to_stemnodes(link))
        lrus[-1][-1]["linked"] = True
        lrus[-1][-1]["crawlDepth"] = crawlMetas["depth"] + 1
        lrus[-1][-1]["crawlTimestamp"] = crawlMetas["timestamp"]
    return lrus

def write_query(session, query, **kwargs):
    return session.write_transaction(lambda tx: tx.run(query, **kwargs))

def read_query(session, query, **kwargs):
    return session.read_transaction(lambda tx: tx.run(query, **kwargs))

def init_neo4j(session, queries):
    write_query(session, queries["drop_db"])
    write_query(session, queries["constrain_lru"])
    write_query(session, queries["startup"])

def load_lrus(session, queries, pages=[]):
    if not pages:
        pages = [(
          "s:http|h:fr|h:sciences-po|h:medialab|p:people|",
          [
            "s:http|h:fr|h:sciences-po|h:medialab|p:projets|",
            "s:http|h:com|h:twitter|p:medialab_ScPo|",
            "s:http|h:com|h:twitter|p:paulanomalie|"
          ],
          {
            "encoding": "utf-8",
            "depth": 0,
            "error": None,
            "status": 200,
            "timestamp": 1472238151623
          }
        )]
    lrus = []
    for lru, lrulinks, metas in pages:
        lrus += prepare_lrus(lru, lrulinks, metas)
    a = write_query(session, queries["index"], lrus=lrus)
    print(a._summary.counters.__dict__)

def run_WE_creation_rule(session, queries, lastcheck):
    we_prefixes = read_query(session, queries["we_default_creation_rule"], lastcheck=lastcheck)
    webentities=[]
    for we_prefixe in we_prefixes:
      lru = we_prefixe[0].properties['lru']
      we = {}
      we['prefixes']=get_alternative_prefixes(lru)
      we['name']=name_lru(lru)
      webentities.append(we)
    result = write_query(session, queries["create_wes"], webentities=webentities)
    print(result._summary.counters.__dict__)


if __name__ == "__main__":
    # Load config
    try:
        import config as cf
    except:
        sys.stderr.write("ERROR: please create & fill config.py from config.py.example first")
        exit(1)

    # MongoDB Connection
    mongoconn = MongoClient(cf.mongo_host, cf.mongo_port)[cf.mongo_base][cf.mongo_coll]

    # Read Neo4J Queries file
    with open("queries/core.cypher") as f:
        queries = read_queries_file(f)

    # Neo4J Connection
    neo4jdriver = GraphDatabase.driver("bolt://%s:%s" % (cf.neo4j_host, cf.neo4j_port), auth=(cf.neo4j_user, cf.neo4j_pass))
    with neo4jdriver.session() as session:
        # ResetDB
        if len(sys.argv) > 1:
            init_neo4j(session, queries)
        print mongoconn.count()
        pages = []
        totalsize = 0
        batchsize = 0
        for page in mongoconn.find({}):
            pages.append((
              page["lru"],
              page["lrulinks"],
              {k: v for k, v in page.items() if k in ["encoding", "error", "depth", "status", "timestamp"]}
            ))
            totalsize += len(page["lrulinks"]) + 1
            batchsize += len(page["lrulinks"]) + 1
            if batchsize >= cf.page_batch:
                load_lrus(session, queries, pages)
                pages = []
                batchzize = 0
        load_lrus(session, queries, pages)
        run_WE_creation_rule(session, queries, 0)
        print totalsize
