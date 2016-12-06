from elasticsearch import Elasticsearch, RequestsHttpConnection
import tweepy
import json
import pprint
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import boto3
from config import *
import json
import re
import sys
from watson_developer_cloud import AlchemyLanguageV1

from requests_aws4auth import AWS4Auth

# CONSUMER - Sends messages to ES
alchemy_language = AlchemyLanguageV1(api_key='74ee0eee2415cab433d5733bb460f6ca49ee5053')


REGION = "us-west-2"

awsauth = AWS4Auth(YOUR_ACCESS_KEY, YOUR_SECRET_KEY, REGION, 'es')
host = "search-tweet-trends-hq2ehcbl6hi5iqyn7xairven3e.us-east-1.es.amazonaws.com"

access_token="28203065-m0YfUbocnLSgTzmfV5kYX7FIhLHo71s9Pb36yu3jB"
access_token_secret="1xc2XNwgXhCEm34NbhDeuIEnPuDAqHkUrG2Wpp7W1p2ge"
consumer_key="euXCzLT4bHep6PMSwFha1X610"
consumer_secret="czLjLODWigoHvUxdXR7KhPoucrTP36HVxZtK19wqDATpQjM3tW"

# es = Elasticsearch(
#   hosts=[{
#     'host': host,
#     'port': 443,
#   }],
#   http_auth=awsauth,
#   use_ssl=True,
#   connection_class=RequestsHttpConnection
#   )
# es.indices.create(index='tweet-sqs', ignore=400)

# print (es.info())

# Get the service resource
sqs = boto3.resource('sqs')

# Get the queue
queue = sqs.get_queue_by_name(QueueName='tweet-queue-main')


while(1):

  # Process messages by printing out body and optional author name
  for message in queue.receive_messages(MessageAttributeNames=['All']):
      # # Get the custom author message attribute if it was set
      # author_text = ''
      # if message.message_attributes is not None:
      #     author_name = message.message_attributes.get('Author').get('StringValue')
      #     if author_name:
      #         author_text = ' ({0})'.format(author_name)
      # name = ''
      # coords = ''
      # print message
      if message.message_attributes is not None:
        name = message.message_attributes.get('Name').get('StringValue')
        coords = message.message_attributes.get('Coordinates').get('StringValue')
        tweet = ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)"," ",message.body).split())
        print ("trying")
        options={'language':'english'}
        try:
          response = alchemy_language.sentiment("text", tweet)
          sent = response['docSentiment']['type']
          # Print out the body and author (if set)
          print('TWEET, {0} {1} {2}'.format(message.body, name, coords))

          data = {
            'text' : tweet,
            'name' : name,
            'coordinates' : coords,
            'sentiment': sent
          }
          print "Final Data: {}\n".format(data)

          client = boto3.client('sns')
          response = client.create_topic(
              Name='tweet_sentiments'
          )
          topic_arn = response['TopicArn']
          print (response)
          # print (response["arn"])
          published_message = client.publish(
              TopicArn=topic_arn,
              Message=json.dumps(data, ensure_ascii=False),
              MessageStructure='string'
          )
          print("We published something!")
          # res = es.index(index="tweet-sqs", doc_type='tweet', body=data)

          # print(res)
        except:
          print (sys.exc_info())
          pass
        # Let the queue know that the message is processed
      message.delete()


