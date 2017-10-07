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
        return self.validate_schema(binlogevent) and self.validate_format(row)

    def validate_schema(self, binlogevent):
        return isinstance(binlogevent, WriteRowsEvent) and binlogevent.schema == schema and binlogevent.table == event_table

    def validate_format(self, row):
        row_vals = row['values']
        return row_vals['topic'] != None
    
    def build_envelope(self, binlogevent, row, log_pos, log_file):
        row_vals = row['values']
        log_metadata = BinLogMetadata(log_pos, log_file, binlogevent.schema, binlogevent.table, row_vals['id'])
        message = MessageEventStore(INSERT_EVENT,binlogevent.schema,binlogevent.table,binlogevent.primary_key,row_vals)
        envelope = KafkaEnvelope(message,log_metadata,row_vals['topic'],row_vals['routing_key'])
        
        return envelope