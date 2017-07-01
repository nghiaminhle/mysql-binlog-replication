from apollo.processors.processor import Processor
from apollo.consumers.kafka_consumer import Consumer
from apollo.consumers.gray_log.gray_log_handler import GrayLogHanlder
from apollo.configurations import monitored_topics

class GrayLogProcessor(Processor):
    _gray_logger = None
    _consumer = None
    _cosumer_group = 'graylog1'

    def __init__(self):
        self._gray_logger = GrayLogHanlder()
        self._consumer = Consumer(monitored_topics, self._cosumer_group)
    
    def start(self):
        self._consumer.start(self._gray_logger)
        return
    
    def stop(self):
        self._consumer.stop()