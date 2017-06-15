from Apollo.Log.BinLog.BinLogCommit.LogCommit import LogCommit
from Apollo.Configurations import kafka_bootstrap_server
from Apollo.Log.BinLog.BinLogReader.BinLogMetadata import BinLogMetadata
from Apollo.Processors.Cancellation import Cancellation
from Apollo.Processors.DynamicThrottling import DynamicThrottling
from Apollo.Log.BinLog.BinLogCommit.CommitHandler import CommitHandler
from Apollo.Monitoring.Tracing import handle_commit_exception
from Apollo.Monitoring.Retry import retry
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

    def __init__(self, commit_events_queue: Queue, throttling: DynamicThrottling):
        self._lock_object = Lock()
        self._throttling = throttling
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
        self._throttling.wait_to_commit(self._cancellation)

    def _handle_commit_exception(exp):
        handle_commit_exception(exp)
        self._throttling.penalize_commit_error()