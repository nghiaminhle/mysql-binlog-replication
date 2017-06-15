from .Envelope import Envelope

class KafkaEnvelope(Envelope):
    # topic name to publish to kafka
    _topic = None
    # partition key to choose partition
    _partition_key = None

    def __init__(self, body, log_metadata, topic, partition_key):
        Envelope.__init__(self, body, log_metadata)
        self._topic = "backorder" #topic
        self._partition_key = partition_key
    
    @property
    def topic(self):
        return self._topic
    
    @topic.setter
    def topic(self, value):
        self._topic = value

    @property
    def partition_key(self):
        return self._partition_key

    @partition_key.setter
    def partition_key(self, value):
        self._partition_key = value