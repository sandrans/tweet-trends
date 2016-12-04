from elasticsearch import Elasticsearch, RequestsHttpConnection
import tweepy
import json
import pprint
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

import boto3

from requests_aws4auth import AWS4Auth


YOUR_ACCESS_KEY = ""
YOUR_SECRET_KEY = ""
REGION = "us-east-1"

awsauth = AWS4Auth(YOUR_ACCESS_KEY, YOUR_SECRET_KEY, REGION, 'sqs')
host = "search-tweet-trends-hq2ehcbl6hi5iqyn7xairven3e.us-east-1.es.amazonaws.com"
access_token="28203065-m0YfUbocnLSgTzmfV5kYX7FIhLHo71s9Pb36yu3jB"
access_token_secret="1xc2XNwgXhCEm34NbhDeuIEnPuDAqHkUrG2Wpp7W1p2ge"
consumer_key="euXCzLT4bHep6PMSwFha1X610"
consumer_secret="czLjLODWigoHvUxdXR7KhPoucrTP36HVxZtK19wqDATpQjM3tW"

# PRODUCER - Fetches from tweepy and creates the queue

# # Get the service resource
# sqs = boto3.resource('sqs')

# # Create the queue. This returns an SQS.Queue instance
# queue = sqs.create_queue(QueueName='test', Attributes={'DelaySeconds': '5'})

# # You can now access identifiers and attributes
# print(queue.url)
# print(queue.attributes.get('DelaySeconds'))

# # queue.send_message(MessageBody='boto3', MessageAttributes={
# #     'Author': {
# #         'StringValue': 'Daniel',
# #         'DataType': 'String'
# #     }
# # })

#session = Session(aws_access_key_id=SQSAccessKey, aws_secret_access_key=SQSSecretKey,region_name=sqsRegion)


# Get the service resource
sqs = boto3.resource('sqs')

# Create the queue. This returns an SQS.Queue instance
# queue = sqs.create_queue(QueueName='tweet-queue', Attributes={'DelaySeconds': '5'})

# You can now access identifiers and attributes
# print(queue.url)
# print(queue.attributes.get('DelaySeconds'))
queue_name='tweet-queue'

# queue = sqs.create_queue(QueueName=queue_name, Attributes={'DelaySeconds': '5'})
queue = sqs.get_queue_by_name(QueueName='tweet-queue')
print(queue.url)
print(queue.attributes.get('DelaySeconds'))


class Producer(StreamListener):

	global queue

	def on_data(self, doc_data):
		json_data=json.loads(doc_data)
		data={}
		attributes = {}
		if json_data.get('coordinates') is not None:
			try:
				data["text"]=str(json_data['text'])
				attributes["coordinates"]= (json_data['coordinates']['coordinates'][0],json_data['coordinates']['coordinates'][1])
				attributes["name"]=str(json_data['user']['name'])
				attributes["created_at"]=json_data['created_at']
				
				print ("printing data...\n")
				print (data)
				print ("printing attributes...\n")
				print (attributes)

				print(queue.url)
				res = queue.send_message(MessageBody=data["text"], MessageAttributes=attributes)
				print(res.get('MessageId'))

				# res = es.index(index="test", doc_type='tweet', body=str(data))
				print("Message sent?")
			except:
				pass

		elif json_data.get('place') is not None:
			try:
				data["text"]=str(json_data['text'])
				attributes["coordinates"]= (json_data['place']['bounding_box']['coordinates'][0][0][0],json_data['place']['bounding_box']['coordinates'][0][0][1])
				attributes["name"]=str(json_data['user']['name'])
				attributes["created_at"]=json_data['created_at']
				print ("printing data...\n")
				print (data)
				print ("printing attributes...\n")
				print (attributes)
				
				print(queue.url)
				queue.send_message(MessageBody=data["text"], MessageAttributes=attributes)
				# res = es.index(index="test", doc_type='tweet', body=str(data))
				print("Message sent?")
			except:
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



