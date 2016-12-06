import json
import requests
import tweepy
import time
from elasticsearch import Elasticsearch, RequestsHttpConnection
from flask import Flask, render_template, request
import certifi
from requests_aws4auth import AWS4Auth
import sys
from config import *

import sns_receiver as sns

application = Flask(__name__)

REGION = "us-west-2"
# host = 'tweetmap-env.bmcf8muqyd.us-west-2.elasticbeanstalk.com'
awsauth = AWS4Auth(YOUR_ACCESS_KEY, YOUR_SECRET_KEY, REGION, 'es')
# host = 'search-tweet-trends-hq2ehcbl6hi5iqyn7xairven3e.us-east-1.es.amazonaws.com'
host = "search-tweet-trends-euni-bv5nxkgvthtwhgtrq6grokr4oi.us-west-2.es.amazonaws.com"
# es = Elasticsearch([host])

# es = Elasticsearch(host=host,port=443,use_ssl=True,ca_certs=certifi.where(),verify_certs=True)
es = Elasticsearch(
  hosts=[{
    'host': host,
    'port': 443,
  }],
  http_auth=awsauth,
  use_ssl=True,
  verify_certs=True,
  connection_class=RequestsHttpConnection
  )

def searching(term):
    if term == 'all':
        #term = "trump"
        elasticcollect = Elasticsearch()
        query = json.dumps({
            "query":{
                "match_all":{}

            }
        })
    else:
        #term = "trump"
        elasticcollect = Elasticsearch()
        query = json.dumps({
            "query": {
                "match": {
                    "text":term
                }

            }
        })
    queryresult = elasticcollect.search(index="test-index", doc_type="tweet", body=query)
    data_return = []
    for doc in queryresult['hits']['hits']:
        data_return.append(doc['_source'])

        #print(queryresult)
    #print(data_return)
    return data_return

@application.route('/')
def index():
    return render_template('index.html')

# Whenever there is a request to this with a key, we get back results from elastic search
@application.route('/tweets/<key>')
def search(key):
    result = es.search(index='test-index',doc_type="tweet",body={
      "from":0,
      "size":100,
      "query":{
        "match": {
          'text': {
            "query":key,
            "operator":"and"
            }
          }
        }
      })
    # print (result)
    data = json.dumps(result['hits']['hits'])
    print "Returned data: {}".format(data)
    # for hit in result['hits']['hits']:
    #   print(hit)

    print ("length", len(result['hits']['hits']))
    return data

@application.route('/notification', methods=['GET','POST'])
def notification():
  print(request.method)
  if (request.method == "POST"):
    # print(key)

    print(request)
    # imd = request.form
    # print(dict(imd))
    # # print(request.data)
    # print("args",request.args.get())
    # # print(request.args.get('data', "clinton"))
    # #request.args.get('data', "clinton"))
    # # sns.notification(data)
    # print("POST notification")
    # data = request.args['data']
    # print(data)
    try:

      # data = request.args['data']
      # print(data)

      # r = requests.get(url)
      # print(r)
      sns.notification(request.data)
    except:
      print("Unexpected error:", sys.exc_info())
      pass
  return "HI"


if __name__ == "__main__":
    application.debug = True
    application.run(port=8000)
