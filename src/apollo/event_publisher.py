from apollo.log.binlog.reader.binlog_reader import BinLogReader
from apollo.log.binlog.commit.kafka_commit import KafkaCommit
from apollo.log.binlog.commit.log_metadata_fetcher import LogMetadataFetcher
from apollo.processors.cancellation import Cancellation
from apollo.processors.dynamic_throttling import DynamicThrottling
from apollo.log.binlog.reader.binlog_handler import BinLogHandler
from apollo.log.binlog.commit.commit_handler import CommitHandler
from apollo.monitoring.monitor import Monitor
from apollo.processors.processor import Processor
from apollo.monitoring.performance_reporter import PerformanceReporter
from apollo.log.binlog.reader.filter.binlog_filter import BinLogFilter
from apollo.log.binlog.reader.filter.binlog_condition_config import DefaultConditionConfiguration
from apollo.log.binlog.commit.checkpoint_committer import CheckPointCommitter
from apollo.log.binlog.reader.kafka_publisher import KafkaPublisher
from apollo.log.binlog.reader.binlog_metadata import BinLogMetadata
from queue import Queue
from threading import Lock
from threading import Thread
import time
from apollo.log.binlog.reader.publisher_processor import PublisherProccessor
import logging
from datetime import datetime

logger = logging.getLogger('general')

class EventPublisher(Processor):
    _throttling = None # DynamicThrottling
    _log_reader = None  # : BinLogReader
    _checkpoint_commiter = None # CheckpointCommitter
    
    _pending_events = Queue(maxsize=0)
    _commit_events = Queue(maxsize=0)

    _cancellation = None
    _lock_object = None
    
    _monitoring = None
    _kafka_publisher = None
    _publisher_processor = None
    _commit_database_enable = False

    def __init__(self, commit_database_enable=False):
        self._commit_database_enable = commit_database_enable
        self._lock_object = Lock()
        self._cancellation = Cancellation()

        metadata_fetcher = LogMetadataFetcher()
        checkpoint = metadata_fetcher.fetch_lastest_checkpoint()
        if checkpoint != None:
            msg = "{} start from {} {}".format(datetime.now(), checkpoint.log_pos, checkpoint.log_file)
            logger.info(msg)

        self._throttling = DynamicThrottling()
        reporter = PerformanceReporter()
        self._monitoring = Monitor(self._pending_events, self._commit_events, self._throttling, reporter, self._cancellation)

        commit_handler = CommitHandler()
        log_commit = KafkaCommit(commit_handler, self._monitoring.get_instrumentation(), self._throttling)
        self._checkpoint_commiter = CheckPointCommitter(log_commit)

        self._monitoring.monitor_commiter(self._checkpoint_commiter)
        
        configuration = DefaultConditionConfiguration()
        filtering = BinLogFilter(configuration)
        self._kafka_publisher = KafkaPublisher()
        binlog_handler = BinLogHandler(filtering, self._kafka_publisher, self._checkpoint_commiter, self._monitoring.get_instrumentation(), self._pending_events)
        self._log_reader = BinLogReader(checkpoint, binlog_handler, self._monitoring.get_instrumentation(), self._throttling)

        self._publisher_processor = PublisherProccessor(self._pending_events, self._kafka_publisher, self._checkpoint_commiter, self._monitoring.get_instrumentation(), self._commit_database_enable)

    def start(self):
        self._throttling.start()
        self._monitoring.start()
        self._publisher_processor.start(self._cancellation)
        self._log_reader.start(self._cancellation)
        self._checkpoint_commiter.start(self._cancellation)
        #block until thread complete
        self._log_reader.join()

    def stop(self):
        self._lock_object.acquire()
        try:
            self._cancellation.cancel()     
        finally:
            self._lock_object.release()
    
    def enable_commit_datbase(self, is_enabled):
        self.enable_commit_datbase = is_enabled

    @property
    def cancellation(self):
        return self._cancellation