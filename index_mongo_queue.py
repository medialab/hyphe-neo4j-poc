#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys, re
from time import time
from lru import split_lru_in_stems, get_alt_prefixes, name_lru, clean_lru
from creationRules import getPreset
from read_queries import read_queries_file
from pymongo import MongoClient
from neo4j.v1 import GraphDatabase
from warnings import filterwarnings
filterwarnings(action='ignore', category=UserWarning,
               message="Bolt over TLS is only available")

TEST_DATA = {
  "pages": [(
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
  )],
  "links": [
    ["s:http|h:fr|h:sciences-po|h:medialab|p:people|",
     "s:http|h:fr|h:sciences-po|h:medialab|p:projets|"],
    ["s:http|h:fr|h:sciences-po|h:medialab|p:people|",
     "s:http|h:com|h:twitter|p:medialab_ScPo|"],
    ["s:http|h:fr|h:sciences-po|h:medialab|p:people|",
     "s:http|h:com|h:twitter|p:paulanomalie|"]
  ],
  "WECRs": [
    {'prefix': '', 'pattern': 'domain'},
    {'prefix': 's:http|h:com|h:twitter|', 'pattern': 'path-1'},
    {'prefix': 's:https|h:com|h:twitter|', 'pattern': 'path-1'},
    {'prefix': 's:http|h:com|h:facebook|', 'pattern': 'path-1'},
    {'prefix': 's:https|h:com|h:facebook|', 'pattern': 'path-1'},
    {'prefix': 's:http|h:com|h:linkedin|', 'pattern': 'path-2'},
    {'prefix': 's:https|h:com|h:linkedin|', 'pattern': 'path-2'}
  ]
}

def write_query(session, query, **kwargs):
    return session.write_transaction(lambda tx: tx.run(query, **kwargs))

def read_query(session, query, **kwargs):
    return session.read_transaction(lambda tx: tx.run(query, **kwargs))

def init_neo4j(session, queries, WECR_method):
    write_query(session, queries["drop_db"])
    write_query(session, queries["constrain_lru"])
    if WECR_method == "afterindex":
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

def init_WE_creation_rules(session, queries, rules=[]):
    # default rule
    if not rules:
        rules = TEST_DATA["WECRs"]
   # prepare regexp for creation rules in runtime
    WECR_regexps = {r['prefix'] + r['pattern']: re.compile(getPreset(r['pattern'], r['prefix'])) for r in rules}
    write_query(session, queries["index_lrus"],lrus = [lru_to_stemnodes(r["prefix"]) for r in rules if r["prefix"]!=""])
    write_query(session, queries["create_wecreationrules"], rules=rules)
    return WECR_regexps

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

def create_webentities(lrus):
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

def load_pages_batch(session, queries, pages=[], links=[], WECR_regexps={}, last_WEs_creation_time=None):
    if not pages:
        pages = TEST_DATA["pages"]
    if not links:
        links = TEST_DATA["links"]

    lrus = []
    for lru, lrulinks, metas in pages:
        lrus += prepare_lrus(lru, lrulinks, metas)

    # pages
    results = write_query(
        session,
        queries["index_lrus" + ("_return_wecr" if WECR_regexps else "")],
        lrus=lrus
    )
    print(results._summary.counters.__dict__)

    # web entities
    new_WEs_creation_time = time()
    if WECR_regexps:
        wetocreate = []
        for r in results.records():
            try:
                we = WECR_regexps[r['prefix']+r['pattern']].match(r['lru']).group(1)
                wetocreate.append(we)
            except AttributeError:
                print "WARNING: error on applying WECR ", WECR_regexps[r['prefix']+r['pattern']], "on", r['lru']
        create_webentities(wetocreate)
    else:
        run_WE_creation_rule(session, queries, last_WEs_creation_time)

    # links
    linkscreation = write_query(session, queries["index_links"], links=links)
    print(linkscreation._summary.counters.__dict__)

    return new_WEs_creation_time


def duration(t, minutes=False):
    t1 = time() - t
    if minutes:
        t1 /= 60.
    return int(round(t1))

def load_batch_from_mongodb(mongoconn, session, queries, lrus_batch_size, WECR_regexps={}):
    print mongoconn.count()
    pages = []
    links = []
    batchsize = 0
    totalsize = 0
    donepages = 0
    t0 = time()
    t = time()
    last_WECR_time = 0
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
            last_WECR_time = load_pages_batch(session, queries, pages, links, WECR_regexps=WECR_regexps, last_WEs_creation_time=last_WECR_time)
            print "TOTAL done:", donepages, "/", totalsize, "this batch:", batchsize, "IN:", duration(t), "s", "/", duration(t0, 1), "min"
            pages = []
            links = []
            batchsize = 0
            t = time()
    load_pages_batch(session, queries, pages, links, WECR_regexps=WECR_regexps, last_WEs_creation_time=last_WECR_time)
    print "TOTAL done:", donepages, "/", totalsize, "this batch:", batchsize, "IN:", duration(t), "s", "/", duration(t0, 1), "min"


if __name__ == "__main__":
    # Load config
    try:
        from config import neo4j_conf, mongo_conf, lrus_batch_size, WECR_method
    except:
        sys.stderr.write("ERROR: please create & fill config.py "
                         "from config.py.example first")
        exit(1)

    # MongoDB Connection
    mongodb = MongoClient(mongo_conf["host"], mongo_conf["port"])
    mongoconn = mongodb[mongo_conf["base"]][mongo_conf["coll"]]
    if "_job" not in [v["key"][0][0] for v in mongoconn.index_information().values()]:
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
            init_neo4j(session, queries, WECR_method)
        if WECR_method == "onindex":
            WECR_regexps = init_WE_creation_rules(session, queries)
        else:
            WECR_regexps = {}
        # Load dummy test data
        load_pages_batch(session, queries, WECR_regexps=WECR_regexps, last_WEs_creation_time=0)
        # Load corpus from MongoDB pages
        #load_batch_from_mongodb(mongoconn, session, queries, lrus_batch_size, WECR_regexps)

