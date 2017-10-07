from apollo.configurations import kafka_bootstrap_server, topic_binlog
from apollo.log.binlog.reader.binlog_metadata import BinLogMetadata
from kafka import KafkaProducer
import json
import time
from apollo.monitoring.tracing import print_commit_message
import logging
from datetime import datetime

class CommitHandler:
    _producer = None  # : KafkaProducer
    _topic = topic_binlog

    def __init__(self):
        self._producer = KafkaProducer(
                                    bootstrap_servers=kafka_bootstrap_server, 
                                    max_in_flight_requests_per_connection=1, 
                                    retries=2147483647, 
                                    acks='all')
    
    def handle(self, log_metadata: BinLogMetadata, fail_callback):
        self._submit_checkpoint(log_metadata, fail_callback)

    def _submit_checkpoint(self, log_metadata, fail_callback):
        logger = logging.getLogger('commit')
        msg = '{} Commit {} {} {} {}'.format(datetime.now(), log_metadata.log_file, log_metadata.log_pos, log_metadata.schema, log_metadata.table)
        logger.info(msg)
        json_payload = json.dumps(
            {'file': log_metadata.log_file, 
            'pos': log_metadata.log_pos, 
            'table': log_metadata.table, 
            'schema': log_metadata.schema}, 
            sort_keys=True)
        future = self._producer.send(self._topic, str.encode(json_payload))     
        future.add_errback(fail_callback, checkpoint=log_metadata)