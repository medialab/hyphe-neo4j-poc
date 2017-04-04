
MATCH (n) DETACH DELETE n;
CREATE CONSTRAINT ON (s:STEM) ASSERT s.lru IS UNIQUE;
CREATE (w:STEM:ROOT {stem:'ROOT' ,lru:''})<-[:parent]-
		(:STEM:SCHEME {stem:'s:http' ,lru:'s:http'})<-[:parent]-
		(:STEM:HOST {stem:'h:fr' ,lru:'s:http|h:fr'})<-[:parent]-
		(:STEM:HOST {stem:'h:sciences-po' ,lru:'s:http|h:fr|h:sciences-po'})<-[:parent]-
		(:STEM:HOST {stem:'h:medialab' ,lru:'s:http|h:fr|h:sciences-po|h:medialab'})<-[:parent]-
		(:STEM:PATH {stem:'p:people' ,lru:'s:http|h:fr|h:sciences-po|h:medialab|p:people'});


MATCH (medialab:STEM:HOST {lru:'s:http|h:fr|h:sciences-po|h:medialab'})
MERGE (medialab)<-[:parent]-(:STEM:PATH {stem:'p:projets'});

MATCH (root:STEM:ROOT {stem:'ROOT' ,lru:''})
CREATE (root)<-[:parent]-
		(:STEM:SCHEME {stem:'s:https' ,lru:'s:https'})<-[:parent]-
		(:STEM:HOST {stem:'h:com' ,lru:'s:https|h:com'})<-[:parent]-
		(:STEM:HOST {stem:'h:twitter' ,lru:'s:http|h:com|h:twitter'})<-[:parent]-
		(:STEM:HOST {stem:'p:medialab_ScPo' ,lru:'s:http|h:com|h:twitter|p:medialab_ScPo'})

match (n)-[r]-(m) return n,r,m;
