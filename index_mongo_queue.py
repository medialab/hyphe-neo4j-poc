#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys, re
from time import time
from pymongo import MongoClient
from neo4j_util import Neo4J
import lru as LRUs
from creationRules import getPreset
from dummy_data import TEST_DATA

def init_DB(neo4j, WECR_method):
    neo4j.write_query("drop_db")
    neo4j.write_query("constrain_lru")
    if WECR_method == "afterindex":
        neo4j.write_query("stem_timestamp_index")
        neo4j.write_query("stem_type_index")
    neo4j.write_query("create_root")

def init_WE_creation_rules(neo4j, rules=TEST_DATA["WECRs"]):
    extended_rules = [{"prefix": prefix, "pattern": r["pattern"]} for r in rules for prefix in LRUs.get_alt_prefixes(r["prefix"])]
   # precompile regexps for creation rules in runtime
    WECR_regexps = {r["prefix"] + r["pattern"]: re.compile(getPreset(r["pattern"], r["prefix"])) for r in extended_rules}
    neo4j.write_query("index_lrus", lrus=[LRUs.lru_to_stemnodes(r["prefix"]) for r in extended_rules if r["prefix"]])
    neo4j.write_query("create_wecreationrules", rules=extended_rules)
    return WECR_regexps

def define_webentities(neo4j, lrus=TEST_DATA["manual_webentities"]):
    wes = [{
      "name": LRUs.name_lru(lru),
      "prefixes": LRUs.get_alt_prefixes(lru)
    } for lru in lrus]
    neo4j.write_query("index_lrus", lrus=[LRUs.lru_to_stemnodes(l) for lru in lrus for l in LRUs.get_alt_prefixes(lru)])
    neo4j.write_query("create_wes", webentities=wes)

def run_WE_creation_rule(neo4j, lastcheck):
    #we_prefixes = neo4j.read_query("we_default_creation_rule", lastcheck=lastcheck)
    we_prefixes = neo4j.read_query("we_apply_creation_rule", lastcheck=lastcheck)
    #lrus = next(we_prefixes.records())["lrus"]
    lrus = [r['lru'] for r in we_prefixes.records()]

    webentities = []
    lrusToCreate = []
    for lru in lrus:
        we = {}
        we['prefixes'] = LRUs.get_alt_prefixes(lru)
        lrusToCreate += we['prefixes']
        we['name'] = LRUs.name_lru(lru)
        webentities.append(we)

    result = neo4j.write_query("index_lrus", lrus=[LRUs.lru_to_stemnodes(lru) for lru in lrusToCreate])
    print(result._summary.counters.__dict__)
    result = neo4j.write_query("create_wes", webentities=webentities)
    print(result._summary.counters.__dict__)

def create_webentities(neo4j, lrus):
    webentities = []
    lrusToCreate = []
    for lru in lrus:
        we = {}
        we['prefixes'] = LRUs.get_alt_prefixes(lru)
        lrusToCreate += we['prefixes']
        we['name'] = LRUs.name_lru(lru)
        webentities.append(we)

    result = neo4j.write_query("index_lrus", lrus=[LRUs.lru_to_stemnodes(lru) for lru in lrusToCreate])
    print(result._summary.counters.__dict__)
    result = neo4j.write_query("create_wes", webentities=webentities)
    print(result._summary.counters.__dict__)

def prepare_lrus(lru, lruLinks, crawlMetas={}):
    lrus = []
    now = int(time())

    lrus.append(LRUs.lru_to_stemnodes(lru))
    lrus[-1][-1]["crawled"] = True
    lrus[-1][-1]["crawlDepth"] = crawlMetas.get("depth", 0)
    lrus[-1][-1]["crawlTimestamp"] = crawlMetas.get("timestamp", now)
    lrus[-1][-1]["crawlHTTPCode"] = crawlMetas.get("status", 200)
    lrus[-1][-1]["crawlError"] = crawlMetas.get("error", None)
    lrus[-1][-1]["pageEncoding"] = crawlMetas.get("encoding", "utf-8")

    for link in lruLinks:
        lrus.append(LRUs.lru_to_stemnodes(link))
        lrus[-1][-1]["linked"] = True
        lrus[-1][-1]["crawlDepth"] = crawlMetas.get("depth", 0) + 1
        lrus[-1][-1]["crawlTimestamp"] = crawlMetas.get("timestamp", now)

    return lrus

def load_pages_batch(neo4j, pages=TEST_DATA["pages"], links=TEST_DATA["links"],
                     WECR_regexps={}, last_WEs_creation_time=None):
    lrus = []
    for lru, lrulinks, metas in pages:
        lrus += prepare_lrus(lru, lrulinks, metas)

    # pages
    results = neo4j.write_query("index_lrus" + ("_return_wecr" if WECR_regexps else ""), lrus=lrus)
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
        create_webentities(neo4j, wetocreate)
    else:
        run_WE_creation_rule(neo4j, last_WEs_creation_time)

    # links
    linkscreation = neo4j.write_query("index_links", links=links)

    print(linkscreation._summary.counters.__dict__)

    return new_WEs_creation_time

def duration(t, minutes=False):
    t1 = time() - t
    if minutes:
        t1 /= 60.
    return int(round(t1))

def load_batch_from_mongodb(mongoconn, neo4j, lrus_batch_size, WECR_regexps={}):
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
            last_WECR_time = load_pages_batch(neo4j, pages, links, WECR_regexps=WECR_regexps, last_WEs_creation_time=last_WECR_time)
            print "TOTAL done:", donepages, "/", totalsize, "this batch:", batchsize, "IN:", duration(t), "s", "/", duration(t0, 1), "min"
            pages = []
            links = []
            batchsize = 0
            t = time()
    load_pages_batch(neo4j, pages, links, WECR_regexps=WECR_regexps, last_WEs_creation_time=last_WECR_time)
    print "TOTAL done:", donepages, "/", totalsize, "this batch:", batchsize, "IN:", duration(t), "s", "/", duration(t0, 1), "min"


if __name__ == "__main__":
    # Load config
    try:
        from config import (neo4j_conf, mongo_conf, lrus_batch_size,
                           WECR_method, load_type)
    except:
        sys.stderr.write("ERROR: please create & fill config.py "
                         "from config.py.example first\n")
        exit(1)

    # MongoDB Connection
    mongodb = MongoClient(mongo_conf["host"], mongo_conf["port"])
    mongoconn = mongodb[mongo_conf["base"]][mongo_conf["coll"]]
    if "_job" not in [v["key"][0][0] for v in mongoconn.index_information().values()]:
        print "Creating index"
        mongoconn.ensure_index("_job")

    # Neo4J Connection
    neo4j = Neo4J(neo4j_conf, "queries/core.cypher")

    # ResetDB
    if len(sys.argv) > 1:
        init_DB(neo4j, WECR_method)
    # Build WECRs
    WECR_regexps = init_WE_creation_rules(neo4j)
    WECR_regexps = WECR_regexps if WECR_method == "onindex" else {}
    if load_type == "dummy":
    # Load dummy test data
        define_webentities(neo4j)
        load_pages_batch(
          neo4j,
          WECR_regexps=WECR_regexps if WECR_method == "onindex" else {},
          last_WEs_creation_time=0
        )
    elif load_type == "mongo":
    # Load corpus from MongoDB pages
        define_webentities(neo4j)
        load_batch_from_mongodb(
          mongoconn,
          neo4j,
          lrus_batch_size,
          WECR_regexps=WECR_regexps
        )
    else:
        sys.stderr.write('ERROR: load_type in config.py should be "dummy" '
                         'or "mongo"\n')
        exit(1)
