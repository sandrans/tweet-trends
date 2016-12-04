from elasticsearch import Elasticsearch, RequestsHttpConnection
import tweepy
import json
import pprint
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import boto3
from requests_aws4auth import AWS4Auth

# CONSUMER - Sends messages to ES

YOUR_ACCESS_KEY = ""
YOUR_SECRET_KEY = ""
REGION = "us-east-1"

awsauth = AWS4Auth(YOUR_ACCESS_KEY, YOUR_SECRET_KEY, REGION, 'es')
host = "search-tweet-trends-hq2ehcbl6hi5iqyn7xairven3e.us-east-1.es.amazonaws.com"

access_token="28203065-m0YfUbocnLSgTzmfV5kYX7FIhLHo71s9Pb36yu3jB"
access_token_secret="1xc2XNwgXhCEm34NbhDeuIEnPuDAqHkUrG2Wpp7W1p2ge"
consumer_key="euXCzLT4bHep6PMSwFha1X610"
consumer_secret="czLjLODWigoHvUxdXR7KhPoucrTP36HVxZtK19wqDATpQjM3tW"

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

# Get the service resource
sqs = boto3.resource('sqs')

# Get the queue
queue = sqs.get_queue_by_name(QueueName='tweet-queue')


while(1):

	# Process messages by printing out body and optional author name
	for message in queue.receive_messages(MessageAttributeNames=['All']):
	    # # Get the custom author message attribute if it was set
	    # author_text = ''
	    # if message.message_attributes is not None:
	    #     author_name = message.message_attributes.get('Author').get('StringValue')
	    #     if author_name:
	    #         author_text = ' ({0})'.format(author_name)
	    name = ''
	    coords = ''
	    if message.message_attributes is not None:
	    	name = message.message_attributes.get('name').get('StringValue')
	    	coords = message.message_attributes.get('coordinates').get('StringValue')

	    # Print out the body and author (if set)
	    print('TWEET, {0}	{1}	{2}'.format(message.body, name, coords))

	    data = {
	    	'text' : message.body,
	    	'name' : name,
	    	'coordinates' : coords
	    }
	    res = es.index(index="test", doc_type='tweet', body=data)

	    print(res)
	    # Let the queue know that the message is processed
	    message.delete()


