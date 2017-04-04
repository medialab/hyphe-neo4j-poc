// name: constrain_lru
// Creating a uniqueness constraint on the stems' LRUs.
CREATE CONSTRAINT ON (s:Stem) ASSERT s.lru IS UNIQUE;

// name: timestamp_index
// Creating an index on the stems' creation timestamp.
CREATE INDEX ON :Stem(createdTimestamp);

// name: startup
// Startup query creating basic nodes such as the ROOT.
MERGE (:Stem {lru: "", stem: "ROOT"});

// name: drop_db
// Handy query truncating the whole Neo4j database.
MATCH (n) DETACH DELETE n;

// name: index
// Indexing a batch of LRUs represented as lists of stems.
UNWIND $lrus AS stems
WITH [{lru: ""}] + stems AS stems
WITH extract(n IN range(1, size(stems) - 1) | {first: stems[n - 1], second: stems[n]}) AS tuples
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
      b:Page
  MERGE (a)<-[:PARENT]-(b)
);

// name: we_default_creation_rule
// Default web entity creation rule.
MATCH (s:Stem)
WHERE
  s.createdTimestamp > $lastcheck AND
  NOT ((s)-[:PREFIX]->(:WebEntity)) AND
  s.lru =~ 's:[a-zA-Z]+\\|(t:[0-9]+\\|)?(h:[^\\|]+\\|(h:[^\\|]+\\|)+|h:(localhost|(\\d{1,3}\\.){3}\\d{1,3}|\\[[\\da-f]*:[\\da-f:]*\\])\\|)'
RETURN s

// name: create_wes
// [[lru1,lru2,lru3,lru4],...]
UNWIND $webentities AS we
WITH we.name AS weName, we.prefixes AS prefixes
MERGE (we:WebEntity {name:weName})
WITH we, prefixes
UNWIND prefixes AS prefixe
MERGE (s:Stem {lru:prefixe})
WITH we,s
MATCH (s)
WHERE NOT (s)-[:PREFIX]->()
CREATE (we)<-[:PREFIX]-(s)
