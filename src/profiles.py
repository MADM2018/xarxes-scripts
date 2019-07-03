import os
import bson

from mongo_client import get_client
from aggregates import aggr_tweets_by_user_id_by_months


def set_all_profiles_months_tweets():
    db = get_client()

    for profile in db.profiles.find():
        if 'id' in profile:
            id = profile['id']
            aggr_tweets_by_user_id_by_months(id)
