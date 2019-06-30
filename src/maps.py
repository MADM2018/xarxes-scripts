import os
import bson

from mongo_client import get_client

geo_keys = ['coordinates', 'geo', 'place']


def hasGeo(tweet):
    for key in geo_keys:
        if key in tweet and tweet[key] is not None:
            return True

    return False


def build_tweets_map():
    db = get_client()

    for tweet in db.tweets.find():

        try:
            if hasGeo(tweet):

                geo_info = {}
                for key in geo_keys:
                    if key in tweet:
                        geo_info[key] = tweet[key]

                geo_info['id'] = tweet['id']

                # insert
                db.maps.update_one({'id': tweet['id']}, {
                                   "$set": geo_info}, upsert=True)
        except:
            pass
