import configparser
from pymongo import MongoClient
import pymongo

def get_db_config():
    config = configparser.ConfigParser()
    config.read('.env')

    db = {}
    section = 'database'

    options = config.options(section)
    for option in options:
        db[option] = config.get(section, option)

    return db

def get_client():
    config = get_db_config()

    client = MongoClient(config['ip'], int(config['port']))

    db = client[config['db']]

    return db