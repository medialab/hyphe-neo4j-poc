// name: tuples
// Generating tuples from LRUs.
UNWIND ["A/B/C", "A/B/D"] AS lrus
WITH split(lrus, '/') AS stems
WITH extract(n IN range(1, size(stems) - 1) | [stems[n - 1], stems[n]]) AS tuples
UNWIND tuples AS tuple
RETURN tuple;

// name: startup
// Startup query creating basic nodes & defining indices
MERGE (root:Root:Stem {lru: ""});

// name: index
// Indexing a list of LRUs
UNWIND $lrus AS stems
WITH [{lru: "", root: true}] + stems AS stems
WITH extract(n IN range(1, size(stems) - 1) | {first: stems[n - 1], second: stems[n]}) AS tuples
UNWIND tuples AS tuple

MATCH (a:Stem {lru: tuple.first.lru})
MERGE (a)<-[:PARENT]-(b:Stem {lru: tuple.second.lru})
  ON CREATE SET
    b.type = tuple.second.type,
    b.stem = tuple.second.stem
FOREACH (_ IN CASE WHEN tuple.second.page THEN [1] ELSE [] END | SET b:Page);
