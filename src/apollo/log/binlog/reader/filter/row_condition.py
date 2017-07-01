from .binlog_condition import BinLogCondition
from .kafka_envelope import KafkaEnvelope
from apollo.log.binlog.reader.binlog_metadata import BinLogMetadata
from pymysqlreplication.row_event import (
    WriteRowsEvent,
    UpdateRowsEvent,
    DeleteRowsEvent
)
from .constants import INSERT_EVENT, UPDATE_EVENT, DELETE_EVENT
from .message_row import InsertedMessage, UpdatedMessage, DeletedMessage

class RowCondition(BinLogCondition):
    _schema = None
    _table = None
    _event_type = None # insert, update, delete
    _topic = None

    def __init__(self, schema, table, event_type, topic):
        self._schema = schema
        self._table = table
        self._event_type = event_type
        self._topic = topic

    def is_satisfy(self, binlogevent, row):
        binlog_event_type = self.get_event_type(binlogevent)
        if (binlog_event_type == self._event_type and binlogevent.schema == self._schema and binlogevent.table == self._table):
            return True
        return False 

    def get_event_type(self, binlogevent):
        if isinstance(binlogevent, WriteRowsEvent):
            return INSERT_EVENT
        if isinstance(binlogevent, UpdateRowsEvent):
            return UPDATE_EVENT
        if isinstance(binlogevent, DeleteRowsEvent):
            return DELETE_EVENT
        return None

class InsertCondition(RowCondition):

    def __init__(self, schema, table, topic):
        super().__init__(schema, table, INSERT_EVENT, topic)
    
    def build_envelope(self, binlogevent, row, log_pos, log_file):
        log_metadata = BinLogMetadata(log_pos, log_file, binlogevent.schema, binlogevent.table)
        message = InsertedMessage(self._event_type,binlogevent.schema,binlogevent.table,binlogevent.primary_key,row["values"])
        envelope = KafkaEnvelope(message,log_metadata,self._topic, message.get_routing_key())
        return envelope

class UpdateCondition(RowCondition):
    _changed_columns = []
    _filterd_columns = []

    def __init__(self, schema, table, topic, changed_columns = None, filtered_columns = None):
        super().__init__(schema, table, UPDATE_EVENT, topic)
        if changed_columns != None:
            self._changed_columns = changed_columns.split(',')
        if filtered_columns != None:
            self._filterd_columns = filtered_columns.split(',')
    
    def is_satisfy(self, binlogevent, row):
        return super().is_satisfy(binlogevent, row) and self.has_changed(row)
    
    def has_changed(self, row):
        if len(self._changed_columns) == 0:
            return True
            
        before_vals = row['before_values']
        after_vals = row['after_values']

        for c in self._changed_columns:
            if c in before_vals.keys() and before_vals[c] != after_vals[c]:
                return True
        return False

    def build_envelope(self, binlogevent, row, log_pos, log_file):
        log_metadata = BinLogMetadata(log_pos, log_file, binlogevent.schema, binlogevent.table)
        message = UpdatedMessage(self._event_type,binlogevent.schema,binlogevent.table,binlogevent.primary_key, self.filter_vals(binlogevent.primary_key, row["before_values"]), self.filter_vals(binlogevent.primary_key, row["after_values"]) )
        envelope = KafkaEnvelope(message, log_metadata, self._topic, message.get_routing_key())
        return envelope
    
    def filter_vals(self, primary_key, vals):
        keys = self.get_keys(primary_key)
        if len(self._filterd_columns) == 0:
            return vals
        filtered_vals = dict()
        for key in keys:
            filtered_vals[key] = vals[key]
        
        for c in self._filterd_columns:
            if c in vals.keys():
                filtered_vals[c] = vals[c]
        return filtered_vals

    def get_keys(self, primary_key):
        values = []
        if primary_key != None:
            if isinstance(primary_key, tuple):
                for key in primary_key:                    
                    values.append(key)
            elif isinstance(primary_key, str):
                values.append(primary_key)
        return values

class DeleteCondition(RowCondition):
    
    def __init__(self, schema, table, topic):
        super().__init__(schema, table, DELETE_EVENT, topic)
    
    def build_envelope(self, binlogevent, row, log_pos, log_file):
        log_metadata = BinLogMetadata(log_pos, log_file, binlogevent.schema, binlogevent.table)
        message = DeletedMessage(self._event_type,binlogevent.schema,binlogevent.table,binlogevent.primary_key,row["values"])
        envelope = KafkaEnvelope(message, log_metadata, self._topic, message.get_routing_key())
        return envelope