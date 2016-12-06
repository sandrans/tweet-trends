from elasticsearch import Elasticsearch, RequestsHttpConnection
import tweepy
import json
import pprint
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from config import *
import sys

import boto3

from requests_aws4auth import AWS4Auth


# YOUR_ACCESS_KEY = ""
# YOUR_SECRET_KEY = ""
REGION = "us-west-2"

awsauth = AWS4Auth(YOUR_ACCESS_KEY, YOUR_SECRET_KEY, REGION, 'sqs')
# print YOUR_ACCESS_KEY
# print YOUR_SECRET_KEY
host = "search-tweet-trends-hq2ehcbl6hi5iqyn7xairven3e.us-east-1.es.amazonaws.com"
access_token="28203065-m0YfUbocnLSgTzmfV5kYX7FIhLHo71s9Pb36yu3jB"
access_token_secret="1xc2XNwgXhCEm34NbhDeuIEnPuDAqHkUrG2Wpp7W1p2ge"
consumer_key="euXCzLT4bHep6PMSwFha1X610"
consumer_secret="czLjLODWigoHvUxdXR7KhPoucrTP36HVxZtK19wqDATpQjM3tW"

# client = boto3.client(
#     'sqs',
#     aws_access_key_id=YOUR_ACCESS_KEY,
#     aws_secret_access_key=YOUR_SECRET_KEY
# )

# queue_name='tweet-queue-main'

# # queue = client.create_queue(QueueName=queue_name, Attributes={'DelaySeconds': '5'})
# sqs = boto3.resource('sqs')
# queue = sqs.get_queue_by_name(QueueName=queue_name)
# queue.send_message(MessageBody="hello world")

# print(queue)
# print(queue.attributes.get('DelaySeconds'))


class Producer(StreamListener):

  def on_data(self, doc_data):
    json_data=json.loads(doc_data)
    data={}
    attributes = {}
    if json_data.get('coordinates') is not None:
      try:
        data["text"]=str(json_data['text'])
        attributes["coordinates"]= ((json_data['coordinates']['coordinates'][0]),(json_data['coordinates']['coordinates'][1]))
        attributes["name"]=str(json_data['user']['name']).encode('utf-8', 'ignore')
        attributes["created_at"]=(json_data['created_at'])

        client = boto3.client(
          'sqs',
          aws_access_key_id=YOUR_ACCESS_KEY,
          aws_secret_access_key=YOUR_SECRET_KEY
        )

        queue_name='tweet-queue-main'
        # queue = client.create_queue(QueueName=queue_name, Attributes={'DelaySeconds': '5'})
        sqs = boto3.resource('sqs')
        queue = sqs.get_queue_by_name(QueueName=queue_name)

        print("this is the queue {}").format(queue)
        queue.send_message(MessageBody="hoohoho")
        # res = queue.send_message(MessageBody=str(data["text"]), MessageAttributes={'tweet': 'yes'})
        res = queue.send_message(MessageBody=str(data["text"]).encode('ascii', 'ignore'), MessageAttributes={
          'Name': {
            'StringValue': '{}'.format(attributes["name"]),
            'DataType': 'String'
          },
          'Coordinates': {
            'StringValue': '{}'.format(attributes["coordinates"]),
            'DataType': 'String'
          },
          'CreatedAt': {
            'StringValue': '{}'.format(attributes["created_at"]),
            'DataType': 'String'
          }
        })
        print(res.get('MessageId'))

        # res = es.index(index="test", doc_type='tweet', body=str(data))
        print("Message sent?")
      except:
        # print
        print sys.exc_info()
        pass

    elif json_data.get('place') is not None:
      try:
        data["text"]=str(json_data['text'])
        attributes["coordinates"]= (json_data['place']['bounding_box']['coordinates'][0][0][0],json_data['place']['bounding_box']['coordinates'][0][0][1])
        attributes["name"]=str(json_data['user']['name']).encode('ascii', 'ignore')
        attributes["created_at"]=json_data['created_at']
        print ("printing data...\n")
        print (data)
        print ("printing attributes...\n")
        print (attributes)

        client = boto3.client(
          'sqs',
          aws_access_key_id=YOUR_ACCESS_KEY,
          aws_secret_access_key=YOUR_SECRET_KEY
        )

        queue_name='tweet-queue-main'
        # queue = client.create_queue(QueueName=queue_name, Attributes={'DelaySeconds': '5'})
        sqs = boto3.resource('sqs')
        queue = sqs.get_queue_by_name(QueueName=queue_name)

        print("this is the queue {}").format(queue)
        # queue.send_message(MessageBody="teehee")
        res = queue.send_message(MessageBody=str(data["text"]).encode('ascii', 'ignore'), MessageAttributes={
          'Name': {
            'StringValue': '{}'.format(attributes["name"]),
            'DataType': 'String'
          },
          'Coordinates': {
            'StringValue': '{}'.format(attributes["coordinates"]),
            'DataType': 'String'
          },
          'CreatedAt': {
            'StringValue': '{}'.format(attributes["created_at"]),
            'DataType': 'String'
          }
        })
        print(res.get('MessageId'))
        # res = es.index(index="test", doc_type='tweet', body=str(data))
        print("Message sent?")
      except:
        print sys.exc_info()

        pass

    else:
      return

  def on_error(self, status):
    print (status)


if __name__ == '__main__':

  listener = Producer()

  auth = OAuthHandler(consumer_key, consumer_secret)
  auth.set_access_token(access_token, access_token_secret)
  stream = Stream(auth, listener)

  stream.filter(track=['haiku', 'poem', 'poetry', 'obama', 'clinton', 'movie', 'review', 'food',
    'film', 'election', 'happy', 'sad', 'farce', 'great', 'amazing', 'why', 'lions', 'moose',
    'tigers', 'lion', 'cubs', 'emus', 'ostriches', 'ostrich', 'head', 'sand', 'cute', 'sad moose',
    'sad clinton', 'happy clinton'])



