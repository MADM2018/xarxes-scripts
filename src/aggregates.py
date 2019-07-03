import os
import bson

from mongo_client import get_client
from utils import get_tweet_month


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


def aggr_tweets_by_user_id_by_months(user_id):
    db = get_client()

    months_acc = [0] * 12

    for tweet in db.tweets.find():
        # retweeted_status.retweet_count

        if 'user' in tweet:
            user = tweet['user']

            if 'id' in user:
                id = user['id']

                if (id == user_id):
                    month = get_tweet_month(tweet) - 1
                    months_acc[month] = months_acc[month] + 1

    return months_acc
