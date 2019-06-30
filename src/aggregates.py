import os
import bson

from mongo_client import get_client


def aggr_hashtags():
    db = get_client()

    hashtags = 0

    for tweet in db.tweets.find():
        # entities.hashtags

        if 'entities' in tweet:
            entities = tweet['entities']

            if 'hashtags' in entities:
                hashtags += len(tweet['entities']['hashtags'])

    # insert
    db.aggregates.update_one({'_id': bson.ObjectId(b'hashtags_num')}, {
                             "$set": {"value": hashtags, "type": "hashtags"}}, upsert=True)


def aggr_retweets():
    db = get_client()

    retweets = 0

    for tweet in db.tweets.find():
        # retweeted_status.retweet_count

        if 'retweeted_status' in tweet:
            retweeted_status = tweet['retweeted_status']

            if 'retweet_count' in retweeted_status:
                retweets += tweet['retweeted_status']['retweet_count']

    # insert
    db.aggregates.update_one({'_id': bson.ObjectId(b'retweets_num')}, {
                             "$set": {"value": retweets, "type": "retweets"}}, upsert=True)
