// name: tuples
// Generating tuples from LRUs.
UNWIND ["A/B/C", "A/B/D"] AS lrus
WITH split(lrus, '/') AS stems
WITH extract(n IN range(1, size(stems) - 1) | [stems[n - 1], stems[n]]) AS tuples
UNWIND tuples AS tuple
RETURN tuple

// name: startup
// Startup query creating basic nodes & defining indices
MERGE (root:Root:Stem {lru: ""})

// name: index
// Indexing a list of LRUs
UNWIND $lrus AS stems
WITH extract(s IN stems | [{lru: "", root: true}] + s) AS stems
WITH extract(n IN range(1, size(stems) - 1) | [stems[n - 1], stems[n]]) AS tuples
UNWIND tuples AS tuple

MATCH (a:Stem {lru: tuple[0].lru})
MERGE (a)<-[:PARENT]-(b:Stem {lru: tuple[1].lru})
  ON CREATE (
    SET b.type = tuple[1].type
    SET b.stem = tuple[1].stem
    FOREACH (_ IN CASE WHEN tuple[1].page THEN [1] ELSE [] END | SET b:Page)
  )
  ON MATCH (
    FOREACH (_ IN CASE WHEN tuple[1].page THEN [1] ELSE [] END | SET b:Page)
  )
RETURN;
