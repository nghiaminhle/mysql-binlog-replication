from apollo.configurations import kafka_bootstrap_server, topic_binlog
from apollo.log.binlog.reader.binlog_metadata import BinLogMetadata
from kafka import KafkaProducer
import json
import time
from apollo.monitoring.tracing import print_commit_message

class CommitHandler:
    _producer = None  # : KafkaProducer
    _topic = topic_binlog

    def __init__(self):
        self._producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_server)
    
    def handle(self, log_metadata: BinLogMetadata):
        self._submit_checkpoint(log_metadata)

    def _submit_checkpoint(self, log_metadata):
        print_commit_message('commit {} {} {} {}'.format(log_metadata.log_file, log_metadata.log_pos, log_metadata.schema, log_metadata.table))
        json_payload = json.dumps({'file': log_metadata.log_file, 'pos': log_metadata.log_pos}, sort_keys=True)
        self._producer.send(self._topic, str.encode(json_payload))        