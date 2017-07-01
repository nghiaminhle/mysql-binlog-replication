import pymongo
from apollo.configurations import mongo_setting

class MongoLogRepository:
    _client = None
    _database = None

    def __init__(self):
        self._client = pymongo.MongoClient(mongo_setting['host'], mongo_setting['port'])
        self._database = self._client[mongo_setting["database"]]
    
    def log(self, values):
        self._database.messages.insert_one(values)