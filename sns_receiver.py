import boto3
from elasticsearch import Elasticsearch, RequestsHttpConnection
import json
from config import *
import sys
import pprint
import ast
from requests_aws4auth import AWS4Auth

REGION="us-west-2"

# req = {
# 	'Type' : 'Notification',
# 	'Message' : {
# 		'text' : "tweet here about clinton or whatever",
# 		'coordinates' : (-30.0, 45.9),
# 		'sentiment' : 'positive'
# 	}
# }

awsauth = AWS4Auth(YOUR_ACCESS_KEY, YOUR_SECRET_KEY, REGION, 'es')
host = "search-tweet-trends-24sebrx4nnqqvhbh3c737rotcq.us-west-2.es.amazonaws.com"
es = Elasticsearch(
  hosts=[{
    'host': host,
    'port': 443,
  }],
  http_auth=awsauth,
  use_ssl=True,
  connection_class=RequestsHttpConnection
  )
es.indices.create(index='test', ignore=400)

print (es.info())

def notification(req):

	client = boto3.client('sns')
	req = req.decode("utf-8")
	req = ast.literal_eval(req)
	print("NOTIFICATION", req)
	if (req["Type"] == 'SubscriptionConfirmation'):
		confirmation = client.confirm_subsciption(TopicArn=req["TopicArn"], Token=req["Token"])
		print(confirmation)
	elif (req["Type"] == 'Notification'):
		msg = req['Message']
		print(msg)

		es = Elasticsearch()
		tweet = {
			'text' : str(msg['text']),
			'name' : str(msg['name']),
            'coordinates' : str(msg['coordinates']),
            'sentiment': str(msg['sentiment'])
		}

		res = es.index(index="test", doc_type='tweet', body=str(tweet))

		print("INDEXED:")
		print(res)
	else:
		pass



