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
            self._pending_queue.put(envelope)
            self._committer.hold_checkpoint(checkpoint)
        #    self._publisher.handle(envelope, self._publish_successful_callback, self._publish_fail_callback)
        return True

    def _commit_not_allowed_event(self, binlogevent, log_pos, log_file):
        checkpoint = BinLogMetadata(log_pos, log_file, binlogevent.schema, binlogevent.table)
        self._committer.commit_checkpoint(checkpoint)

    _successful_callback = None
    _fail_callback = None

    def set_successful_callback(self, successful_callback):
        self._successful_callback = successful_callback

    def set_fail_callback(self, fail_callback):
        self._fail_callback = fail_callback

    def _publish_successful_callback(self, metadata, checkpoint: BinLogMetadata):
        self._committer.release_checkpoint(checkpoint)
        self._instrumentation.publish_to_kafka()
        if (self._successful_callback != None):
            self._successful_callback(checkpoint)

    def _publish_fail_callback(self, metadata, checkpoint: BinLogMetadata):
        print("----------error send to kafka call back-----------")
        if (self._fail_callback != None):
            self._fail_callback(checkpoint)
