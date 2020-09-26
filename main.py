import json
import urllib
import requests
import re
import sys
from datetime import datetime as dt
from politylink.elasticsearch.client import ElasticsearchClient
from politylink.graphql.client import GraphQLClient as PLGraphQLClient

if len(sys.argv)<=1:
  print("Error: domain is not specified.")
  exit

domain = sys.argv[1]
minutes_handler = sys.argv[2]
es_client = ElasticsearchClient()
pl_client = PLGraphQLClient()

def fetch_news():
  filter=""
  gql = '''
    query{{
      News{{
        title,publishedAt{{year,month,day,hour,minute}}, id, domain
      }}
    }}
  '''.format(a=filter)
  result = pl_client.exec(gql)
  return result["data"]["News"]

def match_minutes(text, date):
  json_data=json.dumps({"text": text, "date": date}, ensure_ascii=False)
  r = requests.post(minutes_handler, data=json_data.encode("utf-8"), headers={'Content-Type': 'application/json'})
  return r.json()

count = 0

for news in fetch_news():
    if not domain in news["domain"]:
      continue
    res = es_client.get(news["id"])
    date = news["publishedAt"]
    time = "{}/{}/{} {}:{}".format(date["year"], date["month"], date["day"], date["hour"], date["minute"])
    minutes = match_minutes("、".join([res.title, res.body]), time)
    for minute in minutes['minutes']:
      count += 1
      print(news, minute)  
      res2 = pl_client.exec_merge_news_referred_minutes(news["id"], minute["id"])
      print(res2)
print("{} references are annotated.".format(count))
