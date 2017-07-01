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

class EventPublisher(Processor):
    _throttling = None # DynamicThrottling
    _log_reader = None  # : BinLogReader
    _checkpoint_commiter = None # CheckpointCommitter
    
    _pending_events = Queue(maxsize=0)
    _commit_events = Queue(maxsize=0)

    _cancellation = None
    _lock_object = None
    
    _monitoring = None
    _publisher_thread = None
    _kafka_publisher = None
    

    def __init__(self):
        self._lock_object = Lock()
        self._cancellation = Cancellation()

        metadata_fetcher = LogMetadataFetcher()
        checkpoint = metadata_fetcher.fetch_lastest_checkpoint()
        if checkpoint != None:
            print('start from', checkpoint.log_pos, checkpoint.log_file)
        checkpoint.log_file = '0b74efb26d3c-bin.000009'
        checkpoint.log_pos = 32432136

        self._throttling = DynamicThrottling()
        reporter = PerformanceReporter()
        self._monitoring = Monitor(self._pending_events, self._commit_events, self._throttling, reporter)

        
        commit_handler = CommitHandler()
        log_commit = KafkaCommit(commit_handler, self._monitoring.get_instrumentation(), self._throttling)
        self._checkpoint_commiter = CheckPointCommitter(log_commit)
        
        configuration = DefaultConditionConfiguration()
        filtering = BinLogFilter(configuration)
        self._kafka_publisher = KafkaPublisher()
        binlog_handler = BinLogHandler(filtering, self._kafka_publisher, self._checkpoint_commiter, self._monitoring.get_instrumentation(), self._pending_events)
        self._log_reader = BinLogReader(checkpoint, binlog_handler, self._monitoring.get_instrumentation(), self._throttling)

        self._publisher_thread = Thread(target=self.publish)
        self._publisher_thread.setDaemon(True)

    def start(self):
        self._publisher_thread.start()
        self._throttling.start()
        self._monitoring.start()
        self._log_reader.start(self._cancellation)
        self._checkpoint_commiter.start(self._cancellation)
        return

    def stop(self):
        self._lock_object.acquire()
        try:
            self._cancellation.cancel()
            self._log_reader.stop()
            self._monitoring.stop()  
            self._checkpoint_commiter.stop()          
        finally:
            self._lock_object.release()
    
    def publish(self):
        threshold = 0.0001
        delay = threshold
        while True:
            if not self._pending_events.empty():
                envelope = self._pending_events.get()
                self._kafka_publisher.handle(envelope, self._publish_successful_callback, self._publish_fail_callback)
                self._monitoring.get_instrumentation().publish_to_kafka()
                time.sleep(delay)
                if self._pending_events.qsize() >10000:
                    delay = 0
                    #self._cancellation.cancel()
                if self._pending_events.qsize() <1000:
                    delay = threshold
            else:
                time.sleep(0.1)
    
    def _publish_successful_callback(self, metadata, checkpoint: BinLogMetadata):
        self._checkpoint_commiter.release_checkpoint(checkpoint)
        return
        #self._instrumentation.publish_to_kafka()
        #if (self._successful_callback != None):
        #    self._successful_callback(checkpoint)

    def _publish_fail_callback(self, metadata, checkpoint: BinLogMetadata):
        return
        #print("----------error send to kafka call back-----------")
        #if (self._fail_callback != None):
        #    self._fail_callback(checkpoint)