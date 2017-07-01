from ..message_handler import MessageHandler
from .mongo_log_repo import MongoLogRepository
import json

class MongoLogHandler(MessageHandler):
    _mongo_repo = None

    def __init__(self):
        self._mongo_repo = MongoLogRepository()

    def handle(self, message):
        values = json.loads(message.value.decode())
        values["topic"] = message.topic
        values["partition"] = message.partition
        values["offset"] = message.offset
        print ('consumer', values)
        self._mongo_repo.log(values)