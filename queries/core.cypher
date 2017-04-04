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

MERGE (a:Stem {lru: tuple.first.lru})
MERGE (b:Stem {lru: tuple.second.lru})
MERGE (a)<-[:PARENT]-(b)
  ON CREATE SET
    b.type = tuple.second.type,
    b.stem = tuple.second.stem,
    b.createdTimestamp = timestamp()
FOREACH (_ IN CASE WHEN tuple.second.page THEN [1] ELSE [] END | SET b:Page);

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
UNWIND $webentities as we
WITH we.name as weName, we.prefixes as prefixes
MERGE (we:WebEntity {name:weName})
with we, prefixes
UNWIND prefixes as prefixe
MERGE (s:Stem {lru:prefixe})
with we,s
MATCH (s)
WHERE NOT (s)-[:PREFIX]->()
CREATE (we)<-[:PREFIX]-(s)
