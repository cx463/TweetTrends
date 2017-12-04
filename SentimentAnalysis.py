import json
import boto3
from watson_developer_cloud import NaturalLanguageUnderstandingV1
from watson_developer_cloud.natural_language_understanding_v1 import Features, SentimentOptions


natural_language_understanding = NaturalLanguageUnderstandingV1(
  username="422edc6b-00b5-45ec-a7c0-51d6c8f4b375",
  password="OvaxrE8H78hl",
  version="2017-02-27")

sns_client=boto3.client(
	"sns",
	aws_access_key_id="AKIAJGTIL662VNVQXJ7Q",
    aws_secret_access_key="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",
    region_name="us-east-1")


def handler(event, context):
    #print("Received event: ")
    event=event["Records"][0]["Sns"]["Message"]
    event=json.loads(event)["default"]
    if "requestID" not in event:
        print "error"
        return
    else:
        requestID=event["requestID"]
        count=event["count"]
    tweetDict=json.loads(receiveMessage()["Body"])["default"]

    tweetText=tweetDict["text"]
    sentiment=analyzeSentiment(tweetText)
    tweetDict["sentiment"]=sentiment
    message_to_esindex=json.dumps(
        {"default":
            {"requestID":requestID,
             "count":count,
             "tweet":tweetDict
             }
         }
    )

    sns_response=sns_client.publish(
          TargetArn="arn:aws:sns:us-east-1:966412209653:SentimentAnalysis-ES",
          Message=message_to_esindex
          )
    print sns_response


def analyzeSentiment(tweetText):
    response = natural_language_understanding.analyze(
        text=tweetText,
        features=Features(
    	    sentiment=SentimentOptions()
    	)
    )
    return response["sentiment"]["document"]["label"]



def receiveMessage():
    sqs = boto3.client('sqs', 
    aws_access_key_id="AKIAJGTIL662VNVQXJ7Q",
    aws_secret_access_key="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",
    region_name = "us-east-1"
    )

    queue_url = 'https://sqs.us-east-1.amazonaws.com/966412209653/twittermapQueue.fifo'

    response={}


    response = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=1,
        )

    while "Messages" not in response:
    	print "empty message"

    message = response['Messages'][0]
    receipt_handle = message['ReceiptHandle']

    # Delete received message from queue
    sqs.delete_message(
        QueueUrl=queue_url,
        ReceiptHandle=receipt_handle
    )
    return message


'''
test Doc:

Message={"requestID":"testID","count":"00000"}
Message=json.dumps({'default': (Message)})
event={"Records":[{"Sns":{"Message":Message}}]}
handler(event,None)
'''