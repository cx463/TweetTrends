from __future__ import unicode_literals
from datetime import datetime
from tweepy.streaming import StreamListener
from tweepy.api import API
from tweepy import OAuthHandler
from tweepy import Stream
from requests.packages.urllib3.exceptions import ReadTimeoutError
import boto3
import random
import time
import threading
import json

access_token = "96485252-EKxIkgmidLaFMavkkpcTr3ip6cU33wpVQvbhHDIeU"
access_token_secret = "Qp8z0FKnCjl0V39veKdOuC5JnYicpFOZvbtCkUC8nHRqW"
consumer_key = "OjgGOoX1iJEs3nOlAcj2O1UUg"
consumer_secret = "yewUgn8YmJjKSok0sVVN13l9o5ZaZfUqPY2CKPpSXYE0NbJ6bC"
arn_to_sentiment_analysis="arn:aws:sns:us-east-1:966412209653:TwitterStream-SentimentAnalysis"
client=boto3.client(
    "sns",
    aws_access_key_id="AKIAJGTIL662VNVQXJ7Q",
    aws_secret_access_key="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",
    region_name="us-east-1")




def handler(event, context):
    print("Received event: ")

    event=event["Records"][0]["Sns"]["Message"]
    print event, type(event)

    event=json.loads(event)["default"]
    print event, type(event)

    if "requestID" not in event:
        event["requestID"]="testRequest"

    print event, type(event)

    try:
        return fetchStream(event["searchBox"],event["testPrint"],event["requestID"])  #return a list of json objects
    except:
        print "error in fetchStream"

def fetchStream(searchBox,testPrint,requestID):
    l = StdOutListener(testPrint=testPrint,requestID=requestID)
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l,timeout=1)
    try:
        stream.filter(locations=searchBox)
    except ReadTimeoutError:
        print "Timout"

    
    
class StdOutListener(StreamListener):
    
    def __init__(self,requestID,api=None,testPrint=False):
        self.api = api or API()
        self.count=0
        self.testPrint=testPrint
        self.requestID=requestID

    def on_data(self, data):   ##data is unicode, need to load it to dict object, then if need a str back, dumps the dict object
        if self.testPrint:
            print data
        dataDict=json.loads(data)
        onlyShowLang="en"
        tweetLang=dataDict["lang"].lower()
        keep_this_tweet=True

        if  (tweetLang.find(onlyShowLang)==-1):
            keep_this_tweet=False
        else:
            pass

        if keep_this_tweet:
            print "\nsend for sentimentAnalysis"
            try:
                tweetLoc=dataDict["geo"]["coordinates"]
            except:
                tweetLoc=dataDict["place"]["bounding_box"]["coordinates"][0]
                tmp=[0,0]
                for c in tweetLoc:
                    tmp[1]+=c[0]
                    tmp[0]+=c[1]
                tmp[0]=tmp[0]/4
                tmp[1]=tmp[1]/4
                tweetLoc=tmp
                
            tweetLoc={"lng": tweetLoc[1], "lat": tweetLoc[0]}
            dataDict["processedLocation"]=tweetLoc

            Message=json.dumps({'default': dataDict})
            response = sendToSQS(Message,self.count,self.requestID)
            if len(response):

                self.count+=1
                #send a notice to SNS to notify sentimentAnalysis lambda function

                client.publish(
                TargetArn=arn_to_sentiment_analysis,
                Message=json.dumps({"default": {"requestID":self.requestID,"count":"{:05d}".format(self.count)}})
                )


        return True
    
    def on_error(self, status):
        print(status)


def sendToSQS(Message,count,requestID):
    # Create SQS client
    print Message
    print requestID+"{:05d}".format(count)
    sqs = boto3.client('sqs', 
    aws_access_key_id="AKIAJGTIL662VNVQXJ7Q",
    aws_secret_access_key="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",
    region_name = "us-east-1"
    )

    queue_url = 'https://sqs.us-east-1.amazonaws.com/966412209653/twittermapQueue.fifo'

    # Send message to SQS queue
    response = sqs.send_message(
        QueueUrl=queue_url,
        MessageBody= Message,
        MessageGroupId="MyMessageGroupId1234567890",
        MessageDeduplicationId=requestID+"{:05d}".format(count)
   )
    return response["SequenceNumber"]


