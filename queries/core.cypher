// name: drop_db
// Handy query truncating the whole Neo4j database.
MATCH (n) DETACH DELETE n;

// name: constrain_lru
// Creating a uniqueness constraint on the stems' LRUs.
CREATE CONSTRAINT ON (s:Stem) ASSERT s.lru IS UNIQUE;

// name: stem_timestamp_index
// Creating an index on the stems' creation timestamp.
CREATE INDEX ON :Stem(createdTimestamp);

// name: stem_type_index
// Creating an index on the stems' type.
CREATE INDEX ON :Stem(type);

// name: create_root
// Startup query creating basic nodes such as the ROOT.
MERGE (:Stem {lru: "", stem: "ROOT"});

// name: index_lrus
// Indexing a batch of LRUs represented as lists of stems.
UNWIND $lrus AS stems
WITH [{lru: ""}] + stems AS stems
WITH stems[size(stems)-1].lru as lru, extract(n IN range(1, size(stems) - 1) | {first: stems[n - 1], second: stems[n]}) AS tuples
UNWIND tuples AS tuple

FOREACH (_ IN CASE WHEN NOT coalesce(tuple.second.page, false) THEN [1] ELSE [] END |
  MERGE (a:Stem {lru: tuple.first.lru})
  MERGE (b:Stem {lru: tuple.second.lru})
    ON CREATE SET
      b.type = tuple.second.type,
      b.stem = tuple.second.stem,
      b.createdTimestamp = timestamp()
  MERGE (a)<-[:PARENT]-(b)
)

FOREACH (_ IN CASE WHEN coalesce(tuple.second.page, false) THEN [1] ELSE [] END |
  MERGE (a:Stem {lru: tuple.first.lru})
  MERGE (b:Stem {lru: tuple.second.lru})
    ON CREATE SET
      b.type = tuple.second.type,
      b.stem = tuple.second.stem,
      b.createdTimestamp = timestamp(),
      b.crawledTimestamp = tuple.second.crawlTimestamp,
      b.crawlDepth = tuple.second.crawlDepth,
      b.crawlError = tuple.second.crawlError,
      b.crawlHTTPCode = tuple.second.crawlHTTPCode,
      b.pageEncoding = tuple.second.pageEncoding,
      b.crawled = coalesce(tuple.second.crawled, false),
      b.linked = coalesce(tuple.second.linked, false),
      b:Page
    ON MATCH SET
      b.crawlDepth =
        CASE
          WHEN tuple.second.crawlDepth < b.crawlDepth
          THEN tuple.second.crawlDepth
          ELSE b.crawlDepth
          END,
      b.crawled = coalesce(tuple.second.crawled, b.crawled),
      b.linked = coalesce(tuple.second.linked, b.linked),
      b:Page
  MERGE (a)<-[:PARENT]-(b)
);

// name: index_lrus_return_wecr
// Indexing a batch of LRUs represented as lists of stems.
UNWIND $lrus AS stems
WITH [{lru: ""}] + stems AS stems
WITH stems[size(stems)-1].lru as lru, extract(n IN range(1, size(stems) - 1) | {first: stems[n - 1], second: stems[n]}) AS tuples
UNWIND tuples AS tuple

FOREACH (_ IN CASE WHEN NOT coalesce(tuple.second.page, false) THEN [1] ELSE [] END |
  MERGE (a:Stem {lru: tuple.first.lru})
  MERGE (b:Stem {lru: tuple.second.lru})
    ON CREATE SET
      b.type = tuple.second.type,
      b.stem = tuple.second.stem
  MERGE (a)<-[:PARENT]-(b)
)

FOREACH (_ IN CASE WHEN coalesce(tuple.second.page, false) THEN [1] ELSE [] END |
  MERGE (a:Stem {lru: tuple.first.lru})
  MERGE (b:Stem {lru: tuple.second.lru})
    ON CREATE SET
      b.type = tuple.second.type,
      b.stem = tuple.second.stem,
      b.createdTimestamp = timestamp(),
      b.crawledTimestamp = tuple.second.crawlTimestamp,
      b.crawlDepth = tuple.second.crawlDepth,
      b.crawlError = tuple.second.crawlError,
      b.crawlHTTPCode = tuple.second.crawlHTTPCode,
      b.pageEncoding = tuple.second.pageEncoding,
      b.crawled = coalesce(tuple.second.crawled, false),
      b.linked = coalesce(tuple.second.linked, false),
      b:Page
    ON MATCH SET
      b.crawlDepth =
        CASE
          WHEN tuple.second.crawlDepth < b.crawlDepth
          THEN tuple.second.crawlDepth
          ELSE b.crawlDepth
          END,
      b.crawled = coalesce(tuple.second.crawled, b.crawled),
      b.linked = coalesce(tuple.second.linked, b.linked),
      b:Page
  MERGE (a)<-[:PARENT]-(b)
)
WITH lru, tuple
MATCH (a:Stem {lru: tuple.first.lru})-[:PREFIX]->(wecr:WebEntityCreationRule)
WITH lru, reduce( maxStem = {lru:'', depth:0}, 
                 stem IN COLLECT({depth: size(a.lru),pattern: wecr.pattern, prefix:wecr.prefix}) |
                 CASE WHEN stem.depth >= maxStem.depth
                 THEN stem 
                 ELSE maxStem END)
                 AS wecr
RETURN DISTINCT lru, wecr.pattern as pattern, wecr.prefix as prefix;

// name: index_links
UNWIND $links as link
MATCH (a:Stem {lru:link[0]})
MATCH (b:Stem {lru:link[1]})
CREATE (a)-[:LINK]->(b);

// name: we_default_creation_rule
MATCH (s:Stem)
WHERE 
  s.createdTimestamp > $lastcheck AND
  NOT ((s)-[:PREFIX]->(:WebEntity)) AND
  (
    (s {type:'Host'})-[:PARENT]->(:Stem {type:'Host'})-[:PARENT]->(:Stem {type:'Scheme'})
    OR
    (
      (s {type:'Host'})-[:PARENT]->(:Stem {type:'Scheme'}) AND
      NOT (:Stem {type:'Host'})-[:PARENT]->(s)
    )
    OR
    (
      (s {type:'Path'})-[:PARENT]->(:Stem {lru:'s:http|h:com|h:twitter|'})
    )
  )
RETURN collect(s.lru) AS lrus;

// name: create_wecreationrules
UNWIND $rules as rule
MATCH (prefix:Stem {lru:rule.prefix})
CREATE (prefix)-[:PREFIX]->(:WebEntityCreationRule {pattern:rule.pattern, prefix:rule.prefix})


// name: create_wes
UNWIND $webentities as we
WITH we.name as weName, we.prefixes as prefixes
MERGE (we:WebEntity {name:weName})
with we, prefixes
UNWIND prefixes as prefixe
with we,prefixe
MATCH (s:Stem {lru:prefixe})
WHERE NOT (s)-[:PREFIX]->()
CREATE (we)<-[:PREFIX]-(s);

// name: dump
UNWIND [[{s:'a',lru:'a'},{s:'b',lru:'a:b'}],[{s:'a',lru:'a'},{s:'b',lru:'a:b'},{s:'c',lru:'a:b:c'}]] AS stems
WITH [{lru:''}] + stems AS stems, stems[size(stems)-1].lru as lru
WITH stems, reduce( maxStem = {lru:'', depth:0}, stem IN extract(stem in stems | {depth:size(stem.lru),lru:stem.lru}) | CASE WHEN stem.depth >= maxStem.depth THEN stem ELSE maxStem END) AS pointer, lru
WITH extract(n IN range(1, size(stems) - 1) | {first: stems[n - 1], second: stems[n]}) AS tuples, pointer, lru
UNWIND tuples AS tuple
with lru, pointer, collect(tuple) as _ 
RETURN lru, pointer
