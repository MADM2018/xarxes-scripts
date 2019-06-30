import os
import bson

from mongo_client import get_client

def main():
    db = get_client()

    hashtags = 0

    for tweet in db.tweets.find():
        # entities.hashtags

        if 'entities' in tweet:
            entities = tweet['entities']

            if 'hashtags' in entities:
                hashtags += len(tweet['entities']['hashtags'])

    # insert
    db.aggregates.update_one({'_id': bson.ObjectId(b'hashtags_num')}, {"$set": {"value": hashtags}}, upsert=True)

if __name__ == "__main__":
    main()
