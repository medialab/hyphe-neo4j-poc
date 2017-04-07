TEST_DATA = {
  "pages": [(
    "s:http|h:fr|h:sciences-po|h:medialab|p:people|",
    [
      "s:http|h:fr|h:sciences-po|h:medialab|p:projets|",
      "s:http|h:com|h:twitter|p:medialab_ScPo|",
      "s:http|h:com|h:twitter|p:paulanomalie|",
      "s:http|h:fr|h:sciences-po|h:www|p:bibliotheque|",
      "s:https|h:com|h:twitter|"
    ],
    {
      "encoding": "utf-8",
      "depth": 0,
      "error": None,
      "status": 200,
      "timestamp": int(time())
    }
  )],
  "links": [
    ["s:http|h:fr|h:sciences-po|h:medialab|p:people|",
     "s:http|h:fr|h:sciences-po|h:medialab|p:projets|"],
    ["s:http|h:fr|h:sciences-po|h:medialab|p:people|",
     "s:http|h:com|h:twitter|p:medialab_ScPo|"],
    ["s:http|h:fr|h:sciences-po|h:www|p:bibliotheque|",
     "s:http|h:com|h:twitter|"],
    ["s:http|h:fr|h:sciences-po|h:medialab|p:people|",
     "s:http|h:com|h:twitter|p:paulanomalie|"],
    ["s:http|h:com|h:twitter|p:paulanomalie|",
     "s:https|h:com|h:twitter|"],
    ["s:http|h:com|h:twitter|p:paulanomalie|",
     "s:https|h:com|h:twitter|"]
  ],
  "WECRs": [
    {'prefix': '', 'pattern': 'domain'},
    {'prefix': 's:http|h:com|h:twitter|', 'pattern': 'path-1'},
    {'prefix': 's:http|h:com|h:facebook|', 'pattern': 'path-1'},
    {'prefix': 's:http|h:com|h:linkedin|', 'pattern': 'path-2'}
  ],
  "manual_webentities": [
    "s:http|h:fr|h:sciences-po|h:medialab|",
    "s:http|h:fr|h:sciences-po|h:medialab|h:tools|"
  ]
}

