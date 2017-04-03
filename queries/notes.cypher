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
MERGE (:Stem {lru: ""});

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
    b.stem = tuple.second.stem
FOREACH (_ IN CASE WHEN tuple.second.page THEN [1] ELSE [] END | SET b:Page);
