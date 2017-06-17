from apollo.processors.cancellation import Cancellation
from threading import Timer
from threading import Lock
from threading import Thread
import time

class DynamicThrottling:
    _read_log_delay = 0 # time in miliseconds to read log
    _publish_delay = 0 # time in milisconds to publish message to kafka
    _commit_delay = 0 # time in miliseconds to commit to kafka
    _max_delay = 1 # max delay time in seconds
    _min_delay = 0 # min delay time in seconds
    _penalty_amount = 0.1 # penalty amount in seconds
    _gain_amount = 0.1 # gain amount to restore 
    _restoring_interval = 1 # time in seconds to restore
    _restoring_readlog_timer = None # restoring read log timer
    _restoring_publish_timer = None # restoring publish timer
    _restoring_commit_timer = None # restoring commit timer
    _lock_read_log_object = Lock() # lock to change read log delay time
    _lock_publish_object = Lock() # lock to chang publish delay time
    _lock_commit_object = Lock() # lock to commit delay time

    def __init__(self, restoring_interval = 1, gain_amount = 0.1, penalty_amount = 0.5, init_delay = 0, min_delay = 0, max_delay = 10):
        self._restoring_interval = restoring_interval
        self._gain_amount = gain_amount
        self._penalty_amount = penalty_amount
        self._max_delay = max_delay
        self._min_delay = min_delay
        self._read_log_delay = init_delay
        self._publish_delay = init_delay
        self._commit_delay = init_delay

        self._restoring_readlog_timer = Thread(target=self._restore_read_log)
        self._restoring_readlog_timer.setDaemon(True)

        self._restoring_publish_timer = Thread(target=self._restore_publish)
        self._restoring_publish_timer.setDaemon(True)
        
        self._restoring_commit_timer = Thread(target=self._restore_commit)
        self._restoring_commit_timer.setDaemon(True)
    
    def start(self):
        self._restoring_readlog_timer.start()
        self._restoring_publish_timer.start()
        self._restoring_commit_timer.start()
    
    def _decrease_read_log_delay(self, amount):        
        self._lock_read_log_object.acquire()
        amount = self._read_log_delay - amount
        self._read_log_delay = amount if amount> 0 else self._min_delay
        self._lock_read_log_object.release()
    
    def _increase_read_log_delay(self, amount):
        self._lock_read_log_object.acquire()
        amount = self._read_log_delay + amount
        self._read_log_delay = amount if amount<=self._max_delay else self._max_delay
        self._lock_read_log_object.release()
    
    def _decrease_publish_delay(self, amount):        
        self._lock_publish_object.acquire()
        amount = self._publish_delay - amount
        self._publish_delay = amount if amount>0 else self._min_delay
        self._lock_publish_object.release()
    
    def _increase_publish_delay(self, amount):
        self._lock_publish_object.acquire()
        amount = self._publish_delay + amount
        self._publish_delay = amount if amount<=self._max_delay else self._max_delay
        self._lock_publish_object.release()
    
    def _decrease_commit_delay(self, amount):        
        self._lock_commit_object.acquire()
        amount = self._commit_delay - amount
        self._commit_delay = amount if amount>0 else self._min_delay
        self._lock_commit_object.release()
    
    def _increase_commit_delay(self, amount):
        self._lock_commit_object.acquire()
        amount = self._commit_delay + amount
        self._commit_delay = amount if amount<=self._max_delay else self._max_delay
        self._lock_commit_object.release()


    def _restore_read_log(self):
        while True:
            self._decrease_read_log_delay(self._gain_amount)
            time.sleep(self._restoring_interval)
    
    def _restore_publish(self):
        while True:
            self._decrease_publish_delay(self._gain_amount)
            time.sleep(self._restoring_interval)

    def _restore_commit(self):
        while True:
            self._decrease_commit_delay(self._gain_amount)
            time.sleep(self._restoring_interval)

    def wait_to_read_log(self, cancellation: Cancellation):
        if(not cancellation.is_cancel):
            time.sleep(self._read_log_delay)
    
    def wait_to_publish(self, cancellation: Cancellation):
        if(not cancellation.is_cancel):
            time.sleep(self._publish_delay)
        return
    
    def wait_to_commit(self, cancellation: Cancellation):
        if(not cancellation.is_cancel):
            time.sleep(self._commit_delay)
        return
    
    def penalize(self):
        self._increase_read_log_delay(self._penalty_amount)
        self._increase_publish_delay(self._penalty_amount)
        self._increase_commit_delay(self._penalty_amount)
        return
    
    def penalize_read_error(self):
        self._increase_read_log_delay(self._penalty_amount)
        return
    
    def penalize_publish_error(self):
        self._increase_read_log_delay(self._penalty_amount)
        self._increase_publish_delay(self._penalty_amount)
        return
    
    def penalize_commit_error(self):
        self._increase_read_log_delay(self._penalty_amount)
        self._increase_publish_delay(self._penalty_amount)
        self._increase_commit_delay(self._penalty_amount)
        return
    
    def throttle_read(self):
        self._increase_read_log_delay(self._penalty_amount)
    
    @property
    def read_log_delay(self):
        return self._read_log_delay

    @property
    def publish_delay(self):
        return self._publish_delay
    
    @property
    def commit_delay(self):
        return self._commit_delay
    
    @property
    def max_read_log_delay(self):
        return self._max_read_log_delay
    
    @property
    def max_publish_delay(self):
        return self._max_publish_delay

    @property
    def max_commit_delay(self):
        return self._max_commit_delay
    
    @property
    def penalty_amount(self):
        return self._penalty_amount
    
    @property
    def gain_amount(self):
        return self._gain_amount
    
    @property
    def restoring_interval(self):
        return self._restoring_interval