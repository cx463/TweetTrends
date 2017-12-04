from __future__ import unicode_literals
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from elasticsearch.exceptions import ConnectionTimeout
import json


host='https://search-twittermap-domain-thj2oyyhqvdauqqow5ffdxueve.us-east-1.es.amazonaws.com'
es = Elasticsearch([host])

def handler(event, context):
    # TODO implement
    tweet = event["Records"][0]["Sns"]["Message"]
    tweetDict = json.loads(tweet)["default"]["tweet"]
    requestID = json.loads(tweet)["default"]["requestID"]
    count = json.loads(tweet)["default"]["count"]

    print tweetDict
    print requestID, count
    bulkIndex(tweet,requestID,count)


def bulkIndex(tweet,requestID,count):
    actions = [
        {
        "_index": "tweets",
        "_type": "tweet",
        "_id": requestID+count,
        "_source": tweet
        }
    ]
    try:
        helpers.bulk(es,actions)
    except ConnectionTimeout:
        print "error"
