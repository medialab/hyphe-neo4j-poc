# name: we_default_creation_rule
MATCH (s:Stem)
WHERE 
	NOT ((s)-[:PREFIX]->(:WebEntity)) AND
	s.lru =~ 's:[a-zA-Z]+\\|(t:[0-9]+\\|)?(h:[^\\|]+\\|(h:[^\\|]+\\|)+|h:(localhost|(\\d{1,3}\\.){3}\\d{1,3}|\\[[\\da-f]*:[\\da-f:]*\\])\\|){1}' AND
	s.creationTimestamp > $LASTCHECK

// CREATE (s)-[:PREFIX]->(:WebEntity {name:$NAME})
