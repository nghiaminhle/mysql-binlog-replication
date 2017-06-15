from Apollo.Log.BinLog.BinLogReader.BinLogMetadata import BinLogMetadata
from Apollo.Configurations import kafka_bootstrap_server
from kafka import KafkaConsumer
from kafka import TopicPartition
from Apollo.Configurations import topic_binlog
from Apollo.Configurations import binlog_consumer_group
import json

class LogMetadataFetcher:
    _consumer = None#: KafkaConsumer
    _topic = topic_binlog
    _consumer_group_id = binlog_consumer_group
    _offset_reset = 'earliest'

    def __init__(self):    
        self._consumer = KafkaConsumer(
            bootstrap_servers=kafka_bootstrap_server, 
            group_id=self._consumer_group_id, 
            auto_offset_reset = self._offset_reset,
            enable_auto_commit=True
        )

    def fetch_lastest(self)->BinLogMetadata:
        
        partition = TopicPartition(self._topic, 0)
        self._consumer.assign([partition])
        self._consumer.seek_to_end()
        offset = self._consumer.position(partition)
        if offset == None or offset ==0:
            return None
        self._consumer.seek(partition, offset-1)
        for message in self._consumer:
            metadata_attributes = json.loads(message.value.decode())
            metadata = BinLogMetadata(log_pos=metadata_attributes['pos'], log_file=metadata_attributes['file'])
            return metadata
            
        return None