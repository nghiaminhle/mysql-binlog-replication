from apollo.events.event_handler import EventHandler
from kafka import KafkaProducer
from apollo.configurations import kafka_bootstrap_server
from apollo.monitoring.tracing import print_publish_message
import json
from datetime import datetime

def datetime_handler(x):
    if isinstance(x, datetime):
        return x.__str__()

class KafkaPublisher(EventHandler):
    _producer = None  # KafkaProducer

    def __init__(self):
        self._producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_server)
    
    def handle(self, envelope, success_callback, fail_callback):
        topic = envelope.topic
        partition_key = str.encode(envelope.partition_key) if envelope.partition_key != None else None
        json_body = json.dumps(envelope.body.to_dict(), sort_keys=True, default=datetime_handler)
        
        print_publish_message('publish {} {} {} {} {}'.format(envelope.log_metadata.log_file, envelope.log_metadata.log_pos, topic, partition_key, json_body))
        
        future = self._producer.send(topic, str.encode(json_body), partition_key)
        future.add_callback(success_callback, checkpoint=envelope.log_metadata)
        future.add_errback(fail_callback, checkpoint=envelope.log_metadata)