from .envelope import Envelope

class KafkaEnvelope(Envelope):
    # topic name to publish to kafka
    topic = None
    # partition key to choose partition
    partition_key = None

    def __init__(self, body, log_metadata, topic, partition_key):
        Envelope.__init__(self, body, log_metadata)
        self.topic = topic
        self.partition_key = partition_key
