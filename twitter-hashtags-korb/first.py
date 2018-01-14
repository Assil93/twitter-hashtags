from __future__ import unicode_literals
import boto3
import json
from datetime import datetime
from collections import defaultdict
import base64
import time
import decimal
dynamodb = boto3.resource('dynamodb')
kinesis = boto3.client("kinesis")
shard_id = 'shardId-000000000000' #only one shard
shard_it = kinesis.get_shard_iterator(StreamName="twitter-stream", ShardId=shard_id, ShardIteratorType="LATEST")["ShardIterator"]
event = kinesis.get_records(ShardIterator=shard_it, Limit=100)
def update_dynamo_event_counter(tableName, event_name, longi, dynamodb = boto3.resource(service_name='dynamodb', region_name='us-west-2')):
        table = dynamodb.Table(tableName)
        longil = longi[0]
        latd = longi[1]
        print(longil)
        print(latd)
        if event_name:
					      checkItemExists = table.get_item(
 					           Key={
                					'full_name':event_name
        					    }
					      )					
					      if 'Item' in checkItemExists:
						        response = table.update_item(
							Key={
								'full_name': event_name 
							},
							UpdateExpression="set ftCount  = ftCount + :val",
							ConditionExpression="attribute_exists(full_name)",
							ExpressionAttributeValues={
								':val': decimal.Decimal(1) 	
							},
							ReturnValues="UPDATED_NEW"
						)
					      else: 
                                		response = table.update_item(
                                        		Key={
                                                		'full_name': event_name
                                        		},
                                        		UpdateExpression="set ftCount = :val, latitude = :latd, longitude = :longil",
                                        		ExpressionAttributeValues={
                                                		':val': decimal.Decimal(1),
                                                        ':latd': str(latd),
                                                        ':longil': str(longil)
                                        		},
                                        		ReturnValues="UPDATED_NEW"
                                		)  
def update_dynamo_event_hashtag(tableName, htags, dynamodb = boto3.resource(service_name='dynamodb', region_name='us-west-2')):
	table = dynamodb.Table(tableName)
	if htags:
		for ht in htags:
			htag = ht['text']	
			checkItemExists = table.get_item(
 					           Key={
                					'hashtag':htag
        					    }
					      )					
			if 'Item' in checkItemExists:
						        response = table.update_item(
							Key={
								'hashtag': htag 
							},
							UpdateExpression="set htCount  = htCount + :val",
							ConditionExpression="attribute_exists(hashtag)",
							ExpressionAttributeValues={
								':val': decimal.Decimal(1) 	
							},
							ReturnValues="UPDATED_NEW"
						)
			else: 
                                		response = table.update_item(
                                        		Key={
                                                		'hashtag': htag
                                        		},
                                        		UpdateExpression="set htCount = :val",
                                        		ExpressionAttributeValues={
                                                		':val': decimal.Decimal(1)
                                        		},
                                        		ReturnValues="UPDATED_NEW"
                                		)
def lambda_handler(event,context):
   while 1==1:
   	 event = kinesis.get_records(ShardIterator=shard_it, Limit=100)
   	 shard_it = base64.b64decode.event["NextShardIterator"]
#payload=base64.b64decode(["kinesis"]["data"])
#print("Decoded payload: " + str(payload))
print (event)
time.sleep(1.0)