// name: tuples
// Generating tuples from LRUs.
UNWIND ["A/B/C", "A/B/D"] AS lrus
WITH split(lrus, '/') AS stems
WITH extract(n IN range(1, size(stems) - 1) | [stems[n - 1], stems[n]]) AS tuples
UNWIND tuples AS tuple
RETURN tuple
