from threading import Thread
from threading import Lock
from queue import Queue
import time
from apollo.log.binlog.commit.kafka_commit import KafkaCommit
from apollo.processors.cancellation import Cancellation
from datetime import datetime
import logging
from apollo.configurations import waiting_callback_time

class CheckPointCommitter:
    _waiting_checkpoints = None
    _holding_checkpoints = None
    _release_thread = None
    _kafka_committer = None
    _cancellation = None
    _lock_object = None

    def __init__(self, kafka_committer: KafkaCommit):
        self._waiting_checkpoints = Queue()
        self._holding_checkpoints = dict()
        self._kafka_committer = kafka_committer
        self._release_thread = Thread(target=self._watch)
        self._release_thread.setDaemon(True)
        self._lock_object = Lock()
    
    def start(self, cancellation: Cancellation):
        self._cancellation = cancellation
        self._release_thread.start()
        self._kafka_committer.start(cancellation)
    
    def stop(self):
        self._lock_object.acquire()
        try:
            self._cancellation.cancel()
        finally:
            self._lock_object.release()
    
    def _watch(self):
        while True and not self._cancellation.is_cancel:
            if not self._waiting_checkpoints.empty():
                checkpoint = self._waiting_checkpoints.get()
                self._check_to_release(checkpoint)
            else:
                time.sleep(0.1)
    
    def _check_to_release(self, checkpoint):
        count = 0
        sleep_threshold = 0.1 #100 ms
        limitation = int(waiting_callback_time/sleep_threshold)
        while True and not self._cancellation.is_cancel:
            count = count + 1
            if count == limitation:
                logger = logging.getLogger('general')
                logger.info('{}: Stop commit caused by over waiting time for publishing kafka callback. Overtime is ten seconds. Chechkpoint:{}'.format(datetime.now(), checkpoint.key))
                self.stop()
            if not checkpoint.key in self._holding_checkpoints.keys():
                self._release(checkpoint)
                break
            else:
                time.sleep(sleep_threshold)
    
    def _release(self, checkpoint):
        self._kafka_committer.receive_checkpoint(checkpoint)

    def hold_checkpoint(self, checkpoint):        
        msg = "hold {} {} {}".format(checkpoint.log_file, checkpoint.log_pos, datetime.now())
        logger = logging.getLogger('holding')
        logger.info(msg)

        self._holding_checkpoints[checkpoint.key] = checkpoint
        self._waiting_checkpoints.put(checkpoint)
    
    def release_checkpoint(self, checkpoint):
        msg = "release {} {} {}".format(checkpoint.log_file, checkpoint.log_pos, datetime.now())
        logger = logging.getLogger('release')
        logger.info(msg)
        
        if checkpoint.key in self._holding_checkpoints.keys():
            del self._holding_checkpoints[checkpoint.key]
    
    def commit_checkpoint(self, checkpoint):
        self._waiting_checkpoints.put(checkpoint)
    
    def waiting_checkpoint_counts(self):
        return self._waiting_checkpoints.qsize()
    
    def holding_checkpoint_counts(self):
        return len(self._holding_checkpoints)