#!/usr/bin/env python
# -*- coding: utf-8 -*-

def read_queries_file(queries_file):
    queries = {}
    name = ""
    for line in queries_file.read().split("\n"):
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
