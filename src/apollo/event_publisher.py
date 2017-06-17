from apollo.log.binlog.reader.binlog_reader import BinLogReader
from apollo.producers.producer import Producer
from apollo.log.binlog.commit.kafka_commit import KafkaCommit
from apollo.log.binlog.commit.log_metadata_fetcher import LogMetadataFetcher
from apollo.processors.cancellation import Cancellation
from apollo.processors.dynamic_throttling import DynamicThrottling
from apollo.log.binlog.reader.binlog_filtering import BinLogFiltering
from apollo.producers.publisher_handler import PublisherHandler
from apollo.log.binlog.commit.commit_handler import CommitHandler
from apollo.monitoring.monitor import Monitor
from queue import Queue
from threading import Lock

class EventPublisher:
    _throttling = None # DynamicThrottling
    _log_reader = None  # : BinLogReader
    _producer = None  # : Producer
    _log_commit = None  # : KafkaCommit
    
    _pending_events = Queue(maxsize=0)
    _commit_events = Queue(maxsize=0)

    _cancellation = None
    _lockObject = None
    _binlog_filtering = None
    _publisher_handler = None
    _commit_handler = None
    _monitoring = None

    def __init__(self):
        self._lockObject = Lock()
        self._cancellation = Cancellation()

        metadata_fetcher = LogMetadataFetcher()
        metadata = metadata_fetcher.fetch_lastest()
        if metadata != None:
            print('start from', metadata.log_pos, metadata.log_file)
        
        self._binlog_filtering = BinLogFiltering(self._pending_events, self._commit_events)
        self._publisher_handler = PublisherHandler(self._commit_events)
        self._commit_handler = CommitHandler()

        self._throttling = DynamicThrottling()
        self._monitoring = Monitor(self._pending_events, self._commit_events, self._throttling)
        self._log_reader = BinLogReader(metadata, self._monitoring.get_instrumentation(), self._throttling)
        self._producer = Producer(self._throttling, self._monitoring.get_instrumentation(), self._pending_events)
        self._log_commit = KafkaCommit(self._commit_events, self._monitoring.get_instrumentation(), self._throttling)

    def start(self):
        self._throttling.start()
        self._monitoring.start()
        self._log_reader.start(self._binlog_filtering, self._cancellation)
        self._producer.start(self._publisher_handler, self._cancellation)
        self._log_commit.start(self._commit_handler,  self._cancellation)
        return

    def stop(self):
        self._lockObject.acquire()
        try:
            self._cancellation.cancel()
            self._log_reader.stop()
            self._producer.stop()
            self._log_commit.stop()
            self._monitoring.stop()            
        finally:
            self._lockObject.release()