from queue import Queue
from kafka import KafkaProducer
from Apollo.Configurations import kafka_bootstrap_server
from Apollo.Monitoring.Tracing import print_publish_message

class PublisherHandler:
    _async_commit_events = Queue(maxsize=0)
    _producer = None  # KafkaProducer

    def __init__(self, commit_queue: Queue):
        self._async_commit_events = commit_queue
        self._producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_server)
    
    def handle(self, envelope):
        topic = envelope.topic
        partition_key = envelope.partition_key
        print_publish_message('publish {} {} {} {} {} {}'.format(envelope.log_metadata.log_file, envelope.log_metadata.log_pos, topic, partition_key, envelope.body.message_id, envelope.body.payload))
        
        future = self._producer.send(topic, str.encode(envelope.body.payload), str.encode(partition_key))
        future.add_callback(self._publish_successful_callback, log_metadata=envelope.log_metadata)
        future.add_errback(self._publish_fail_callback, log_metadata=envelope.log_metadata)

    def _publish_successful_callback(self, metadata, log_metadata):
        self._async_commit_events.put(log_metadata)
    
    def _publish_fail_callback(self, metadata, log_metadata):
        print("error send to kafka call back")