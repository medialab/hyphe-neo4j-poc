MATCH (we:WebEntity)<-[:PREFIX]-(:Stem)<-[rels:PARENT*0..]-(s:Stem)
WHERE ALL(s IN extract(rel in rels | endNode(rel)) WHERE NOT (s)-[:PREFIX]->())

MATCH (s)-[:LINK]-()-[:PARENT*0..]->()-[:PREFIX]->(twe:WebEntity)
WHERE twe <> we
RETURN DISTINCT we, twe;
