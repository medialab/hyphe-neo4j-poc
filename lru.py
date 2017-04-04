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

def get_alternative_prefixes(lru):
    stems = split_lru_in_stems(lru)
    schemes = [s for s in stems if s[0] == 's']
    hosts = [s  for s in stems if s[0] == 'h']
    ports = [s for s in stems if s[0] == 't']
    paths = [s for s in stems if s[0] == 'p']
    queries = [s for s in stems if s[0] == 'q']
    fragments = [s for s in stems if s[0] == 'f']

    altSchemes = []
    for s in schemes :
        s = list(s)
        if s[1] == 'http':
            s[1] = 'https'
        elif s[1] == 'https':
            s[1] = 'http'  
        altSchemes.append(s)
    altHosts = []
    if len(hosts)>1:
        if 'www' in [h[1] for h in hosts] :
            altHosts = [h for h in hosts if h[1] != 'www']
        else:
            altHosts = [h for h in hosts]
            altHosts.append(('h','www','h:www'))

    prefixes = [lru]
    # alternative alt scheme, same host
    prefixes.append("|".join(":".join(s[0:2]) for s in altSchemes + ports + hosts + paths + queries + fragments)+"|")
    if len(altHosts)>1:
        # alternative same scheme, alt host
        prefixes.append("|".join(":".join(s[0:2]) for s in schemes + ports + altHosts + paths + queries + fragments)+"|")
        # alternative alt scheme, alt host
        prefixes.append("|".join(":".join(s[0:2]) for s in altSchemes + ports + altHosts + paths + queries + fragments)+"|")
    return prefixes

def name_lru(lru):
    host = []
    path = ""
    name = ""
    lasthost = ""
    pathdone = False
    for (k,v,_) in split_lru_in_stems(lru):
        if k == "h" and v != "www":
            lasthost = v.title()
            if host or len(lasthost) > 3:
                host.insert(0, lasthost)
        elif k == "p" and v:
            path = " %s/%s" % ("/..." if pathdone else "", v)
            pathdone = True
        elif k == "q" and v:
            name += ' ?%s' % v
        elif k == "f" and v:
            name += ' #%s' % v
    if not host and lasthost:
        host = [lasthost]
    return ".".join(host) + path + name



if __name__ == "__main__":
    lrus = ["s:http|h:fr|h:sciences-po|h:medialab|",
       's:https|h:com|h:twitter|p:paulanomalie|',
       's:https|h:192.168.0.1|p:paulanomalie|'
       ]
    for lru in lrus:
        print "prefixes for %s :"%lru
        for p in get_alternative_prefixes(lru):
            print p
        print "\n"