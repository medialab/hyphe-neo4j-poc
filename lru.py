#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re, urllib

lruStems = re.compile(r'(?:^|\|)([shtpqf]):')
special_hosts = re.compile(r'localhost|(\d{1,3}\.){3}\d{1,3}|\[[\da-f]*:[\da-f:]*\]', re.I)
re_host_trailing_slash = re.compile(r'(h:[^\|]*)\|p:\|?$')

def split_lru_in_stems(lru):
    elements = lruStems.split(lru.rstrip("|"))
    if len(elements) < 2 or elements[0] != '' or ((len(elements) < 6 or elements[1] != 's' or elements[5] != 'h') and not special_hosts.match(elements[4])):
        raise ValueError("ERROR: %s is not a proper LRU." % lru)
    return [(elements[1+2*i], elements[2+2*i], "%s:%s" % (elements[1+2*i], elements[2+2*i])) for i in range(int(len(elements[1:])/2))]

def clean_lru(lru):
    # Removing trailing slash if ending with path or host
    lru = re_host_trailing_slash.sub(r'\1', lru)
    # Split LRU in stems
    return "|".join([stem.lower() if typ in ['s', 'h'] else stem
        for typ, val, stem in split_lru_in_stems(lru)
        if not stem in ['t:80', ':443']
    ]) + "|"

