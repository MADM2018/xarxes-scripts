import os
import bson

from mongo_client import get_client
from aggregates import aggr_tweets_by_user_id_by_months


def set_all_profiles_months_tweets():
    db = get_client()

    months_acc = {}

    for profile in db.profiles.find():
        if 'id' in profile:
            id = profile['id']

            months = aggr_tweets_by_user_id_by_months(id)
            months_acc[str(int(id))] = months

    # insert
    db.aggregates.update_one({'_id': bson.ObjectId(b'tweets_month')}, {
                             "$set": {"type": "tweets_month", "values": months_acc}}, upsert=True)
