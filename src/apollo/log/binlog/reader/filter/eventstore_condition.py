from .binlog_condition import BinLogCondition
from apollo.configurations import schema, event_table
from .kafka_envelope import KafkaEnvelope
from apollo.log.binlog.reader.binlog_metadata import BinLogMetadata
from .message_eventstore import MessageEventStore
from .constants import INSERT_EVENT
from pymysqlreplication.row_event import (
    WriteRowsEvent
)
# condition only for table event store 
class EventStoreCondition(BinLogCondition):

    def is_satisfy(self, binlogevent, row):
        if isinstance(binlogevent, WriteRowsEvent) and binlogevent.schema == schema and binlogevent.table == event_table:
            return True
        return False
    
    def build_envelope(self, binlogevent, row, log_pos, log_file):
        log_metadata = BinLogMetadata(log_pos, log_file, binlogevent.schema, binlogevent.table)
        row_vals = row['values']
        message = MessageEventStore(INSERT_EVENT,binlogevent.schema,binlogevent.table,binlogevent.primary_key,row_vals)
        envelope = KafkaEnvelope(message,log_metadata,row_vals['topic'],row_vals['routing_key'])
        
        return envelope