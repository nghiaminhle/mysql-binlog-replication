from queue import Queue
from Apollo.Configurations import event_table
from Apollo.Log.BinLog.BinLogReader.BinLogMetadata import BinLogMetadata
from Apollo.Log.BinLog.BinLogReader.KafkaEnvelopeBuilder import KafkaEnvelopeBuilder
from Apollo.Log.BinLog.BinLogReader.BinLog import BinLog
from Apollo.Monitoring.Tracing import print_log_reader

from pymysqlreplication.row_event import (
    WriteRowsEvent
)

class BinLogFiltering:

    _pending_events = Queue(maxsize=0)
    _commit_queue = Queue(maxsize=0)

    def __init__(self,  pending_event_queue: Queue, commit_queue: Queue):
        self._pending_events = pending_event_queue
        self._commit_queue = commit_queue
    
    def filter(self, binlogevent, row, log_pos, log_file):
        if self._is_allowed_event(binlogevent, row, log_pos, log_file):
            return True
        self._commit_not_allowed_event(binlogevent, log_pos, log_file)
        return False
    
    def _is_allowed_event(self, binlogevent, row, log_pos, log_file):
        if isinstance(binlogevent, WriteRowsEvent) and binlogevent.table == event_table:
            envelope = self._build_envelope(row, log_file, log_pos, binlogevent.schema, binlogevent.table)
            self._send_envelope(envelope)
            return True

        return False
    
    def _commit_not_allowed_event(self, binlogevent, log_pos, log_file):
        metadata = BinLogMetadata(log_pos=log_pos, log_file=log_file, schema=binlogevent.schema, table=binlogevent.table)
        self._commit_queue.put(metadata)

    def _build_envelope(self, row, log_file, log_pos, schema, table):
        vals = row["values"]
        print_log_reader('row value:{} {} {} {} {}'.format(vals, log_file, log_pos, schema, table))
        bin_log = BinLog(schema, table, vals, log_file, log_pos)
        kafka_envelope_builder = KafkaEnvelopeBuilder()
        envelope = kafka_envelope_builder.build(bin_log)
        return envelope

    def _send_envelope(self, envelope):
        self._pending_events.put(envelope)