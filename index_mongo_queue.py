#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
from neo4j.v1 import GraphDatabase

if __name__ == "__main__":
    try:
        from config import neo4j_host, neo4j_port, neo4j_user, neo4j_pass
    except Exception as e:
        print type(e), e
        sys.stderr.write("ERROR: please create & fill config.py from config.py.example first")
        exit(1)
    neo4jdriver = GraphDatabase.driver("bolt://%s:%s" % (neo4j_host, neo4j_port), auth=(neo4j_user, neo4j_pass))

