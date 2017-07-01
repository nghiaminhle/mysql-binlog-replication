from apollo.processors.processor import Processor
from apollo.consumers.kafka_consumer import Consumer
from apollo.consumers.mongo.mongo_log_handler import MongoLogHandler
from apollo.configurations import monitored_topics

class MongoLogProcessor(Processor):
    _mongo_logger = None
    _consumer = None
    _consumer_group = 'mongo_logger'

    def __init__(self):
        self._mongo_logger = MongoLogHandler()
        self._consumer = Consumer(monitored_topics, self._consumer_group)

    def start(self):
        self._consumer.start(self._mongo_logger)        
    
    def stop(self):
        self._consumer.stop()