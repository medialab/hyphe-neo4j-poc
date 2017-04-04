// name: tuples
// Generating tuples from LRUs.
UNWIND ["A/B/C", "A/B/D"] AS lrus
WITH split(lrus, '/') AS stems
WITH extract(n IN range(1, size(stems) - 1) | [stems[n - 1], stems[n]]) AS tuples
UNWIND tuples AS tuple
RETURN tuple;

// name: constrain_lru
CREATE CONSTRAINT ON (s:Stem) ASSERT s.lru IS UNIQUE;

// name: startup
// Startup query creating basic nodes & defining indices
MERGE (:Stem {lru: "", stem:"ROOT"});

// name: drop_db
MATCH (n) DETACH DELETE n;

// name: index
// Indexing a list of LRUs
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
RETURN s

// name: create_wes
UNWIND $webentities as we
WITH we.name as weName, we.prefixes as prefixes
MERGE (we:WebEntity {name:weName})
with we, prefixes
UNWIND prefixes as prefixe
with we,prefixe
MATCH (s:Stem {lru:prefixe})
WHERE NOT (s)-[:PREFIX]->()
CREATE (we)<-[:PREFIX]-(s)

