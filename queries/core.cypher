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
MERGE (:Stem:Corpus {lru: "", stem: "ROOT"});

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
MATCH (a:Stem {lru: tuple.first.lru})-[:PREFIX]->(wecr)
WHERE wecr:WebEntityCreationRule OR wecr:WebEntity
WITH lru, reduce( maxStem = {lru:'', depth:0},
                 stem IN COLLECT({depth: size(a.lru),pattern: wecr.pattern, prefix:wecr.prefix}) |
                 CASE WHEN stem.depth >= maxStem.depth
                 THEN stem
                 ELSE maxStem END)
                 AS wecr
WHERE wecr.pattern IS NOT NULL AND wecr.prefix IS NOT NULL
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
    OR
    (
      (s {type:'Path'})-[:PARENT]->(:Stem {lru:'s:https|h:com|h:twitter|'})
    )
    OR
    (
      (s {type:'Path'})-[:PARENT]->(:Stem {lru:'s:http|h:com|h:facebook|'})
    )
    OR
    (
      (s {type:'Path'})-[:PARENT]->(:Stem {lru:'s:https|h:com|h:facebook|'})
    )
    OR
    (
      (s:Stem {type:'Path'})-[:PARENT*2]->(:Stem {lru:'s:http|h:com|h:linkedin|'})
    )
    OR
    (
      (s:Stem {type:'Path'})-[:PARENT*2]->(:Stem {lru:'s:https|h:com|h:linkedin|'})
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

// name: get_webentity_links
MATCH (we:WebEntity)<-[:PREFIX]-(:Stem)<-[rels:PARENT*0..]-(p:Page)
WHERE ALL(s IN extract(rel in rels | endNode(rel)) WHERE NOT (s)-[:PREFIX]->(:WebEntity))
MATCH (p)-[l:LINK]->(:Page)-[:PARENT*0..]->(:Stem)-[:PREFIX]->(twe:WebEntity)
WHERE twe <> we
RETURN DISTINCT id(we) as Source, we.name as Source_label, id(twe) as Target, twe.name as Target_label, count(l) as weight;

// name: get_webentity_links_v2
MATCH (source:WebEntity)<-[:PREFIX]-(:Stem)<-[:PARENT*0..]-(stem:Stem)
WHERE (stem)-[:PREFIX]->(source) OR NOT (stem)-[:PREFIX]->(:WebEntity)
WITH source, stem AS sourcePage
WHERE sourcePage:Page

MATCH (sourcePage)-[:LINK]->(targetPage:Page)
MATCH path=shortestPath((targetPage)-[:PARENT*0..]->(targetStem:Stem))
WHERE (targetStem)-[:PREFIX]->(:WebEntity)
WITH source, path, targetStem
MATCH (targetStem)-[:PREFIX]->(target:WebEntity)
WHERE source <> target
RETURN source, target, count(path) AS weight;

// name: get_webentity_links_v2bis
MATCH (source:WebEntity)<-[:PREFIX]-(:Stem)<-[:PARENT*0..]-(stem:Stem)
WHERE (stem)-[:PREFIX]->(source) OR NOT (stem)-[:PREFIX]->(:WebEntity)
WITH source, stem AS sourcePage
WHERE stem:Page
MATCH (sourcePage)-[:LINK]->(targetPage:Page)
WITH source, targetPage

MATCH (targetPage)<-[:PARENT*0..]-(inner:Stem)-[:PREFIX]->(target:WebEntity )
WHERE inner = targetPage OR NOT (inner)-[:PREFIX]->(:WebEntity)
RETURN source.name, target.name, count(*) AS weight
ORDER BY source.name;

// name: get_webentity_links_v3
MATCH (source:WebEntity), (target:WebEntity)
WHERE
  source <> target AND
  (source)<-[:PREFIX]-(:Stem)<-[:PARENT*0..]-(:Stem)-[:LINK]->(:Page)-[:PARENT*0..]->(:Stem)-[:PREFIX]->(target)
RETURN source, target;

// name: get_webentity_links_v4
MATCH (source:WebEntity)<-[:PREFIX]-(:Stem)<-[:PARENT*0..]-(:Stem)-[:LINK]->(:Page)-[:PARENT*0..]->(:Stem)-[:PREFIX]->(target:WebEntity)
WHERE source <> target
RETURN source, target, count(*) AS weight;

// name: get_webentity_links_v5
MATCH (page:Page)<-[:PARENT*0..]-(:Stem)-[:PREFIX]->(we:WebEntity)
WITH collect([page.lru, we.name]) AS pairs
WITH apoc.map.fromPairs(pairs) AS map

MATCH (sourcePage:Page)-[:LINK]->(targetPage:Page)
WITH map[sourcePage.lru] AS source, map[targetPage.lru] AS target
WHERE source <> target
RETURN source, target, count(*) AS weight;

// name: dump
UNWIND [[{s:'a',lru:'a'},{s:'b',lru:'a:b'}],[{s:'a',lru:'a'},{s:'b',lru:'a:b'},{s:'c',lru:'a:b:c'}]] AS stems
WITH [{lru:''}] + stems AS stems, stems[size(stems)-1].lru as lru
WITH stems, reduce( maxStem = {lru:'', depth:0}, stem IN extract(stem in stems | {depth:size(stem.lru),lru:stem.lru}) | CASE WHEN stem.depth >= maxStem.depth THEN stem ELSE maxStem END) AS pointer, lru
WITH extract(n IN range(1, size(stems) - 1) | {first: stems[n - 1], second: stems[n]}) AS tuples, pointer, lru
UNWIND tuples AS tuple
with lru, pointer, collect(tuple) as _
RETURN lru, pointer

// name: we_apply_creation_rule
MATCH (:WebEntityCreationRule {pattern:'domain'})
      <-[:PREFIX]-(p:Stem)
      <-[:PARENT*0..2]-(h:Stem)
      <-[:PARENT*2]-(s:Stem {type:'Host'})
WHERE (h.type = 'Scheme' OR h.type = 'Port') AND
NOT (s)-[:PREFIX]->(:WebEntity) AND
s.createdTimestamp > $lastcheck
RETURN s.lru as lru
UNION ALL
MATCH (:WebEntityCreationRule {pattern:'domain'})
      <-[:PREFIX]-(p:Stem)
      <-[:PARENT*0..2]-(h:Stem)
      <-[:PARENT]-(s:Stem {type:'Host'})
WHERE (h.type = 'Scheme' OR h.type = 'Port') AND
NOT (s)-[:PREFIX]->(:WebEntity) AND
NOT (s)<-[:PARENT]-(:Stem {type:'Host'}) AND
s.createdTimestamp > $lastcheck
RETURN s.lru as lru
UNION ALL
MATCH (wecr:WebEntityCreationRule {pattern:'path-1'})
      <-[:PREFIX]-(p:Stem)
      <-[:PARENT*0..]-(h:Stem)
      <-[:PARENT*1]-(s:Stem {type:'Path'})
WHERE (h.type = 'Scheme' OR h.type = 'Port' OR h.type = 'Host') AND
NOT (s)-[:PREFIX]->(:WebEntity) AND
s.createdTimestamp > $lastcheck
RETURN s.lru as lru
UNION ALL
MATCH (wecr:WebEntityCreationRule {pattern:'path-2'})
      <-[:PREFIX]-(p:Stem)
      <-[:PARENT*0..]-(h:Stem)
      <-[:PARENT*2]-(s:Stem {type:'Path'})
WHERE (h.type = 'Scheme' OR h.type = 'Port' OR h.type = 'Host') AND
NOT (s)-[:PREFIX]->(:WebEntity) AND
s.createdTimestamp > $lastcheck
RETURN s.lru as lru
