from queue import Queue
from apollo.configurations import event_table
from apollo.log.binlog.reader.binlog_metadata import BinLogMetadata
from apollo.log.binlog.reader.binlog import BinLog
from apollo.monitoring.tracing import print_log_reader
from apollo.log.binlog.reader.filter.binlog_filter import BinLogFilter
from apollo.log.binlog.commit.checkpoint_committer import CheckPointCommitter
from apollo.monitoring.instrumentation import Instrumentation
from pymysqlreplication.row_event import (
    WriteRowsEvent
)
from .kafka_publisher import KafkaPublisher


class BinLogHandler:
    _publisher = None
    _filter = None
    _committer = None
    _instrumentation = None
    _pending_queue = None
    _batch_envelopes = []

    def __init__(self, filter: BinLogFilter, publisher: KafkaPublisher, committer: CheckPointCommitter, instrumentation: Instrumentation, pending_queue: Queue):
        self._publisher = publisher
        self._filter = filter
        self._committer = committer
        self._instrumentation = instrumentation
        self._pending_queue = pending_queue

    def handle(self, binlogevent, row, log_pos, log_file):
        if self._is_allowed_event(binlogevent, row, log_pos, log_file):
            return True
        self._commit_not_allowed_event(binlogevent, log_pos, log_file)
        return False

    def _is_allowed_event(self, binlogevent, row, log_pos, log_file):
        envelopes = self._filter.filter(binlogevent, row, log_pos, log_file)
        if len(envelopes) == 0:
            return False
        for envelope in envelopes:
            checkpoint = BinLogMetadata(log_pos, log_file, binlogevent.schema, binlogevent.table)
            self._committer.hold_checkpoint(checkpoint)
            self._pending_queue.put(envelope)
        
        return True

    def _commit_not_allowed_event(self, binlogevent, log_pos, log_file):
        checkpoint = BinLogMetadata(log_pos, log_file, binlogevent.schema, binlogevent.table)
        self._committer.commit_checkpoint(checkpoint)
