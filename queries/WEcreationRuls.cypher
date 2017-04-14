
MATCH (:WebEntityCreationRule {pattern:'domain'})
			<-[:PREFIX]-(p:Stem)
			<-[:PARENT*0..2]-(h:Stem)
			<-[:PARENT*2]-(s:Stem {type:'Host'})
WHERE (h.type = 'Scheme' OR h.type = 'Port') AND
NOT (s)-[:PREFIX]->(:WebEntity)
RETURN s.lru as lru
UNION ALL
MATCH (:WebEntityCreationRule {pattern:'domain'})
			<-[:PREFIX]-(p:Stem)
			<-[:PARENT*0..2]-(h:Stem)
			<-[:PARENT]-(s:Stem {type:'Host'})
WHERE (h.type = 'Scheme' OR h.type = 'Port') AND
NOT (s)-[:PREFIX]->(:WebEntity) AND
NOT (s)<-[:PARENT]-(:Stem {type:'Host'})
RETURN s.lru as lru
UNION ALL
MATCH (wecr:WebEntityCreationRule {pattern:'path-1'})
			<-[:PREFIX]-(p:Stem)
			<-[:PARENT*0..]-(h:Stem)
			<-[:PARENT*1]-(s:Stem {type:'Path'})
WHERE (h.type = 'Scheme' OR h.type = 'Port' OR h.type = 'Host') AND
NOT (s)-[:PREFIX]->(:WebEntity)
RETURN s.lru as lru
UNION ALL
MATCH (wecr:WebEntityCreationRule {pattern:'path-2'})
			<-[:PREFIX]-(p:Stem)
			<-[:PARENT*0..]-(h:Stem)
			<-[:PARENT*2]-(s:Stem {type:'Path'})
WHERE (h.type = 'Scheme' OR h.type = 'Port' OR h.type = 'Host') AND
NOT (s)-[:PREFIX]->(:WebEntity)
RETURN s.lru as lru