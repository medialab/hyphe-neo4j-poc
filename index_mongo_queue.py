#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys, re
from time import time
from lru import split_lru_in_stems, get_alt_prefixes, name_lru, clean_lru
from read_queries import read_queries_file
from pymongo import MongoClient
from neo4j.v1 import GraphDatabase
from warnings import filterwarnings
filterwarnings(action='ignore', category=UserWarning,
               message="Bolt over TLS is only available")

def write_query(session, query, **kwargs):
    return session.write_transaction(lambda tx: tx.run(query, **kwargs))

def read_query(session, query, **kwargs):
    return session.read_transaction(lambda tx: tx.run(query, **kwargs))

def init_neo4j(session, queries):
    write_query(session, queries["drop_db"])
    write_query(session, queries["constrain_lru"])
    write_query(session, queries["stem_timestamp_index"])
    write_query(session, queries["stem_type_index"])
    write_query(session, queries["create_root"])

stemTypes = {
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
          "type": stemTypes[typ],
          "stem": val
        })
    stems[-1]["page"] = True
    return stems

def prepare_lrus(lru, lruLinks, crawlMetas={}):
    lrus = []
    now = int(time())

    lrus.append(lru_to_stemnodes(lru))
    lrus[-1][-1]["crawled"] = True
    lrus[-1][-1]["crawlDepth"] = crawlMetas.get("depth", 0)
    lrus[-1][-1]["crawlTimestamp"] = crawlMetas.get("timestamp", now)
    lrus[-1][-1]["crawlHTTPCode"] = crawlMetas.get("status", 200)
    lrus[-1][-1]["crawlError"] = crawlMetas.get("error", None)
    lrus[-1][-1]["pageEncoding"] = crawlMetas.get("encoding", "utf-8")

    for link in lruLinks:
        lrus.append(lru_to_stemnodes(link))
        lrus[-1][-1]["linked"] = True
        lrus[-1][-1]["crawlDepth"] = crawlMetas.get("depth", 0) + 1
        lrus[-1][-1]["crawlTimestamp"] = crawlMetas.get("timestamp", now)

    return lrus

def load_pages_batch(session, queries, pages=[], links=[]):
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
            "timestamp": int(time())
          }
        )]

    if not links:
        links = [
          ["s:http|h:fr|h:sciences-po|h:medialab|p:people|",
           "s:http|h:fr|h:sciences-po|h:medialab|p:projets|"],
          ["s:http|h:fr|h:sciences-po|h:medialab|p:people|",
           "s:http|h:com|h:twitter|p:medialab_ScPo|"],
          ["s:http|h:fr|h:sciences-po|h:medialab|p:people|",
           "s:http|h:com|h:twitter|p:paulanomalie|"]
        ]

    lrus = []
    for lru, lrulinks, metas in pages:
        lrus += prepare_lrus(lru, lrulinks, metas)

    a = write_query(session, queries["index_lrus"], lrus=lrus)
    print(a._summary.counters.__dict__)
    a = write_query(session, queries["index_links"], links=links)
    print(a._summary.counters.__dict__)

def duration(t, minutes=False):
    t1 = time() - t
    if minutes:
        t1 /= 60.
    return int(round(t1))


def load_batch_from_mongodb(mongoconn, session, queries, lrus_batch_size):
    print mongoconn.count()
    pages = []
    links = []
    batchsize = 0
    totalsize = 0
    donepages = 0
    t0 = time()
    t = time()
    last_WEs_creation_time = 0
    for page in mongoconn.find({}, sort=[("_job", 1)]):
        pages.append((
          page["lru"],
          page["lrulinks"],
          {k: v for k, v in page.items()
            if k in ["encoding", "error", "depth", "status", "timestamp"]}
        ))
        donepages += 1
        for link in page["lrulinks"]:
            links.append([page["lru"], link])
        batchsize += len(page["lrulinks"]) + 1
        totalsize += len(page["lrulinks"]) + 1
        if batchsize >= lrus_batch_size:
            load_pages_batch(session, queries, pages, links)
            new_WEs_creation_time = time()
            run_WE_creation_rule(session, queries, last_WEs_creation_time)
            last_WEs_creation_time = new_WEs_creation_time
            print "TOTAL done:", donepages, "/", totalsize, "this batch:", batchsize, "IN:", duration(t), "s", "/", duration(t0, 1), "min"
            pages = []
            links = []
            batchsize = 0
            t = time()
    load_pages_batch(session, queries, pages, links)
    print "TOTAL done:", donepages, "/", totalsize, "this batch:", batchsize, "IN:", duration(t), "s", "/", duration(t0, 1), "min"

def run_WE_creation_rule(session, queries, lastcheck):
    we_prefixes = read_query(session, queries["we_default_creation_rule"],
                             lastcheck=lastcheck)
    lrus = next(we_prefixes.records())["lrus"]
    webentities = []
    lrusToCreate = []
    for lru in lrus:
        we = {}
        we['prefixes'] = get_alt_prefixes(lru)
        lrusToCreate += we['prefixes']
        we['name'] = name_lru(lru)
        webentities.append(we)

    result = write_query(session, queries["index_lrus"],
                         lrus=[lru_to_stemnodes(lru) for lru in lrusToCreate])
    print(result._summary.counters.__dict__)
    result = write_query(session, queries["create_wes"],
                         webentities=webentities)
    print(result._summary.counters.__dict__)

def init_WE_creation_rule(session, queries):
  # default rule
  rules =[
    {'lru':'','regexp':'s:[a-zA-Z]+\\|(t:[0-9]+\\|)?(h:[^\\|]+\\|(h:[^\\|]+\\|)+|h:(localhost|(\\d{1,3}\\.){3}\\d{1,3}|\\[[\\da-f]*:[\\da-f:]*\\])\\|)'},
    {'lru':'s:http|h:com|h:twitter|','regexp':'(.*?|h:twitter|p:.*?)|.*'} 
  ]
  write_query(session,queries["index_lrus"],lrus = [lru_to_stemnodes(r["lru"]) for r in rules if r["lru"]!=""])
  write_query(session, queries["create_wecreationrules"], rules=rules)


if __name__ == "__main__":
    # Load config
    try:
        from config import neo4j_conf, mongo_conf, lrus_batch_size
    except:
        sys.stderr.write("ERROR: please create & fill config.py "
                         "from config.py.example first")
        exit(1)

    # MongoDB Connection
    mongodb = MongoClient(mongo_conf["host"], mongo_conf["port"])
    mongoconn = mongodb[mongo_conf["base"]][mongo_conf["coll"]]
    print "Creating index"
    mongoconn.ensure_index("_job")

    # Read Neo4J Queries file
    with open("queries/core.cypher") as f:
        queries = read_queries_file(f)

    # Neo4J Connection
    neo4jdriver = GraphDatabase.driver(
      "bolt://%s:%s" % (neo4j_conf["host"], neo4j_conf["port"]),
      auth=(neo4j_conf["user"], neo4j_conf["pass"])
    )
    with neo4jdriver.session() as session:
        # ResetDB
        if len(sys.argv) > 1:
            init_neo4j(session, queries)
        init_WE_creation_rule(session, queries)
        #load_batch_from_mongodb(mongoconn, session, queries, lrus_batch_size)
        load_pages_batch(session, queries)
        #run_WE_creation_rule(session, queries, 0)
