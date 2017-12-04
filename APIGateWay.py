from __future__ import unicode_literals
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from elasticsearch.exceptions import ConnectionTimeout
import json
import time
import boto3
import random
import string

host='https://search-twittermap-domain-thj2oyyhqvdauqqow5ffdxueve.us-east-1.es.amazonaws.com'
es = Elasticsearch([host])
sns_client=boto3.client(
	"sns",
	aws_access_key_id="",
    aws_secret_access_key="",
    region_name="us-east-1")

def handler(event,context):
    ### Code to extract requestID from event
#    requestID=''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(6))
#    requestID=str(requestID)
    requestContent=event["queryStringParameters"]
    print json.dumps(requestContent)
    requestID=event["requestContext"]["requestId"]
    searchBox=requestContent["Coord"].strip("()").split(",")
    boxSize=1.0
    searchBox=[float(searchBox[1]), float(searchBox[0])]
    searchBox=[searchBox[0]-boxSize, searchBox[1]-boxSize,searchBox[0]+boxSize, searchBox[1]+boxSize]
    print "searchBox: ",searchBox
    
    ##TODO: send SNS to streamTweets Lambda function to start process
    Message={
    "searchBox": searchBox,
    "testPrint": True,
    "requestID":requestID
    }

    Message=json.dumps({'default': Message})
    sns_response=sns_client.publish(
          TargetArn="arn:aws:sns:us-east-1:966412209653:API-TwitterStream",
          Message=Message
          )

    time.sleep(8)
    tweets=search(es,requestID,requestContent) ##list of dicts
    time_no_new_tweets=5
    retryCount=20
    while time_no_new_tweets and retryCount:
        tweetsNew=search(es,requestID,requestContent)
        if len(tweets)<len(tweetsNew):
            print "still getting new tweets"
            tweets=tweetsNew
            print len(tweets)
            time_no_new_tweets=5
        else:
            time_no_new_tweets-=1
        time.sleep(1)
        retryCount-=1
    print len(tweets)
    tweets=[tweet["_source"]["default"]["tweet"] for tweet in tweets if len(tweet["_id"])==len(requestID)+5]
    features={"user_name":["user","name"],"user_screen_name":["user","screen_name"],"text":["text"],"hashtags":["entities","hashtags"],"coord":["processedLocation"],"location":["user","location"],"sentiment":["sentiment"]}
    results=[]
    for tweet in tweets:
        tmp={}
        for feature in features:
            l=features[feature]  ##is a list
            mapping=tweet
            try:
                for x in l:
                    mapping=mapping[x]
                tmp[feature]=mapping
            except:
            	tmp[feature]="unknown"
        if tmp["hashtags"]!="unknown":
            if len(tmp["hashtags"]):
                tmp["hashtags"]=[hashtag["text"] for hashtag in tmp["hashtags"]]
            else:
            	tmp["hashtags"]=None
        results.append(tmp)
    print json.dumps(results)
    response_to_apigateway={
    "isBase64Encoded": False,
    "statusCode": 200,
    "headers": { "Access-Control-Allow-Origin": "*"},
    "body": json.dumps(results)
}
    return response_to_apigateway

##return list of dicts
def search(es,requestID,requestContent):
    doc = {
        "size": "500",
        "query": {
            "bool":{
                "must":[
                    {"query_string": {"default_field":"default.tweet.user.screen_name", "query":"*"+requestContent["User"]+"*"}},
                    {"query_string": {"default_field":"default.tweet.user.name", "query":"*"+requestContent["User"]+"*"}},
                    {"query_string": {
                        "default_field":"default.tweet.entities.hashtags.text", 
                        "query":"*"+str(requestContent["Hashtag"])+"*"} 
                    } if  len(str(requestContent["Hashtag"])) else {},
                    {"query_string": {"default_field":"default.tweet.text", "query":"*"+requestContent["keyword"]+"*"}},
                    {"wildcard": {"_id":requestID+"*"} }
                ]
                
                  
            }       
        }
    }
    print json.dumps(doc)
    res = es.search(index="tweets", doc_type="tweet", body=doc,ignore=[400, 404])
    res=[doc for doc in res["hits"]["hits"]]
    return res
