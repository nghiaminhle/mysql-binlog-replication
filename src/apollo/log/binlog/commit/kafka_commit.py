from apollo.log.binlog.commit.log_commit import LogCommit
from apollo.configurations import kafka_bootstrap_server, limit_commit_check_point_time, limit_pending_checkpoint_number
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
    _cancellation = None
    _lock_object = None
    _throttling = None
    _handler = None
    _instrumentation = None
    _auto_commit_thread = None

    def __init__(self, handler: CommitHandler, instrumentation: Instrumentation, throttling: DynamicThrottling):
        self._lock_object = Lock()
        self._throttling = throttling
        self._instrumentation = instrumentation
        self._handler = handler

    def start(self, cancellation: Cancellation):
        self._cancellation = cancellation
        self._auto_commit_thread = Thread(target=self._schedule_commit)
        self._auto_commit_thread.setDaemon(True)
        self._auto_commit_thread.start()

    def stop(self):
        self._lock_object.acquire()
        try:
            self._cancellation.cancel()
        finally:
            self._lock_object.release()
    
    _last_commit_time = None
    _last_check_point = None
    _pending_number = 0
    _limit_pending_number = limit_pending_checkpoint_number # peding pos for committing
    _limit_pending_time = limit_commit_check_point_time # in seconds, for commiting  
    
    def receive_checkpoint(self, checkpoint):
        self._lock_object.acquire()
        try:
            self._last_check_point = checkpoint
            self._last_commit_time = time.time()
            self._pending_number = self._pending_number + 1
            if (self._pending_number >= self._limit_pending_number):          
                retry(self._commit_checkpoint, self._handle_commit_exception, self._handle_retry_exceeded, checkpoint)
                self._reset()
        finally:
            self._lock_object.release()

    def _commit_checkpoint(self, log_metadata):
        self._handler.handle(log_metadata)
        self._instrumentation.commit_to_kafka()
        self._throttling.wait_to_commit(self._cancellation)

    def _handle_commit_exception(self, exp):
        handle_commit_exception(exp)
        self._throttling.penalize_commit_error()
    
    def _handle_retry_exceeded(self):
        print('retry exceeded')
        self.stop()
    
    def _schedule_commit(self):
        while True and not self._cancellation.is_cancel:
            self._lock_object.acquire()
            try:
                current = time.time()
                if (self._last_commit_time != None and (current - self._last_commit_time) > 5 
                    and self._pending_number>0 and self._last_check_point!=None):
                    retry(self._commit_checkpoint, self._handle_commit_exception, self._handle_retry_exceeded, self._last_check_point)
                    self._reset()
            finally:
                self._lock_object.release()
            time.sleep(1)

    def _reset(self):
        self._last_check_point = None
        self._last_commit_time = None
        self._pending_number = 0