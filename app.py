import json
import requests
import tweepy
import time
from elasticsearch import Elasticsearch, RequestsHttpConnection
from flask import Flask, render_template, request
import certifi
from requests_aws4auth import AWS4Auth
import sys

import sns_receiver as sns

app = Flask(__name__)

YOUR_ACCESS_KEY = "AKIAIGWEBBHB5DVEXPLA"
YOUR_SECRET_KEY = "AfQ206a07F5tDQwX1pdfatsWBrW6ygNzRF0cmMxz"
REGION = "us-east-1"
# host = 'tweetmap-env.bmcf8muqyd.us-west-2.elasticbeanstalk.com'
awsauth = AWS4Auth(YOUR_ACCESS_KEY, YOUR_SECRET_KEY, REGION, 'es')
host = 'search-tweet-trends-hq2ehcbl6hi5iqyn7xairven3e.us-east-1.es.amazonaws.com'
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

@app.route('/')
def index():
    return render_template('index.html')

# Whenever there is a request to this with a key, we get back results from elastic search
@app.route('/tweets/<key>')
def search(key):
    result = es.search(index='test',doc_type="tweet",body={
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
    # for hit in result['hits']['hits']:
    #   print(hit)

    print ("length", len(result['hits']['hits']))
    return data

@app.route('/notification', methods=['GET','POST'])
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
      print("Unexpected error:", sys.exc_info()[0])
  return "HI"


if __name__ == "__main__":
    app.debug = True
    app.run(port=8000)
