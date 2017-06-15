from Apollo.Configurations import kafka_bootstrap_server, topic_binlog
from Apollo.Log.BinLog.BinLogReader.BinLogMetadata import BinLogMetadata
from kafka import KafkaProducer
import json
import time
from Apollo.Monitoring.Tracing import print_commit_message

class CommitHandler:
    _producer = None  # : KafkaProducer
    _topic = topic_binlog
    _last_pos = 0
    _pending_number = 0
    _limit = 5
    _last_commit = None
    _limit_commit_time = 5 # in seconds

    def __init__(self):
        self._producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_server)
    
    def handle(self, log_metadata: BinLogMetadata):
        self._last_pos = log_metadata.log_pos if log_metadata.log_pos> self._last_pos else self._last_pos
        self._pending_number = self._pending_number + 1
        if self._last_commit == None:
            self._last_commit = time.clock()
        current_time = time.clock()
        if self._pending_number> self._limit or (current_time-self._last_commit)>self._limit_commit_time:
            print_commit_message('commit {} {} '.format(log_metadata.log_file, log_metadata.log_pos))
            json_payload = json.dumps(
                {'file': log_metadata.log_file, 'pos': log_metadata.log_pos}, sort_keys=True)
            self._producer.send(self._topic, str.encode(json_payload))        

            self._pending_number = 0
            self._last_commit = time.clock()