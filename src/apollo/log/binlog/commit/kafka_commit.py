from apollo.log.binlog.commit.log_commit import LogCommit
from apollo.configurations import kafka_bootstrap_server
from apollo.log.binlog.reader.binlog_metadata import BinLogMetadata
from apollo.processors.cancellation import Cancellation
from apollo.processors.dynamic_throttling import DynamicThrottling
from apollo.log.binlog.commit.commit_handler import CommitHandler
from apollo.monitoring.tracing import handle_commit_exception
from apollo.monitoring.retry import retry
from apollo.monitoring.instrumentation import Instrumentation
from threading import Lock
from queue import Queue
from kafka import KafkaProducer
from threading import Thread
import json
import time

class KafkaCommit(LogCommit):
    _commit_thread = None  # : Thread
    _commit_queue = None  # : Queue
    _cancellation = None
    _lock_object = None
    _throttling = None
    _handler = None
    _instrumentation = None

    def __init__(self, commit_events_queue: Queue, instrumentation: Instrumentation, throttling: DynamicThrottling):
        self._lock_object = Lock()
        self._throttling = throttling
        self._instrumentation = instrumentation
        self._commit_queue = commit_events_queue

    def start(self, handler: CommitHandler, cancellation: Cancellation):
        self._handler = handler
        self._cancellation = cancellation
        self._commit_thread = Thread(target=self._do_commit)
        self._commit_thread.setDaemon(True)
        self._commit_thread.start()

    def stop(self):
        self._lock_object.acquire()
        try:
            self._cancellation.cancel()
        finally:
            self._lock_object.release()

    def _do_commit(self):
        while True and not self._cancellation.is_cancel:
            if not self._commit_queue.empty():
                log_metadata = self._commit_queue.get()
                retry(self._commit_metadata, self._handle_commit_exception, self.stop, log_metadata)
            else:
                time.sleep(0.1)
        print('stop kafka log commit')
        return

    def _commit_metadata(self, log_metadata):
        self._handler.handle(log_metadata)
        self._instrumentation.commit_to_kafka()
        self._throttling.wait_to_commit(self._cancellation)

    def _handle_commit_exception(exp):
        handle_commit_exception(exp)
        self._throttling.penalize_commit_error()