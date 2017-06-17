from threading import Thread
import time
from apollo.monitoring.instrumentation import Instrumentation
from apollo.processors.dynamic_throttling import DynamicThrottling
from apollo.configurations import limitation_pending_queue

class Monitor:
    _monitored_queues = None
    _monitoring_thread = None
    _cancel = False
    _instrumentation = None

    CONST_PENDING_QUEUE = 'pending_queue'
    CONST_COMMIT_QUEUE = 'commit_queue'
    LIMITATION_PENDING_QUEUE = limitation_pending_queue
    _throttling = None

    def __init__(self, pending_queue, publish_queue, throttling):
        self._monitored_queues = dict()
        self._monitored_queues[self.CONST_PENDING_QUEUE] = pending_queue
        self._monitored_queues[self.CONST_COMMIT_QUEUE] = publish_queue
        self._instrumentation = Instrumentation()
        
        self._monitoring_thread = Thread(target=self._do_monitor)
        self._monitoring_thread.setDaemon(True)
        self._throttling = throttling

    def start(self):
        self._monitoring_thread.start()

    def stop(self):
        self._cancel = True    

    _total_read_log = 0
    _total_filter = 0
    _total_publish = 0
    _total_commit = 0

    def _do_monitor(self):
        while True and not self._cancel:
            self.report()
            self._manage_queue()
            time.sleep(1)
    
    def _manage_queue(self):
        if self._monitored_queues[self.CONST_PENDING_QUEUE].qsize() > self.LIMITATION_PENDING_QUEUE:
            self._throttling.throttle_read()
    
    def report(self):
        read_log_per_seconds = self._instrumentation.read_log_counter.count - self._total_read_log
        self._total_read_log = self._instrumentation.read_log_counter.count
        
        filter_per_seconds = self._instrumentation.filter_counter.count - self._total_filter
        self._total_filter = self._instrumentation.filter_counter.count

        pubish_per_seconds = self._instrumentation.publish_kafka_counter.count - self._total_publish
        self._total_publish = self._instrumentation.publish_kafka_counter.count

        commit_per_seconds = self._instrumentation.commit_counter.count - self._total_commit
        self._total_commit = self._instrumentation.commit_counter.count
        print_format = self._get_print_format()
        print(print_format.format(
                self._monitored_queues[self.CONST_PENDING_QUEUE].qsize(), 
                self._monitored_queues[self.CONST_COMMIT_QUEUE].qsize(),
                self._total_read_log,
                read_log_per_seconds,
                self._total_filter,
                filter_per_seconds,
                self._total_publish,
                pubish_per_seconds,
                self._total_commit,
                commit_per_seconds
                )
            )

    def _get_print_format(self):
        print_format = """
            ------------Monitor------------
            1. Pending Queue:{}
            2. Commit Queue:{}
            3. Total Read Log: {}
            4. Read Log Per Seconds: {}
            5. Total Filter Log: {}
            6. Filter Log Per Seconds: {}
            7. Total Publish To Kafka: {}
            8. Publish To Kafka Per Seconds: {}
            9. Total Commit: {}
            10. Total Commit Per Seconds: {}
            -------------------------------
             """

        return print_format
    
    def get_instrumentation(self):
        return self._instrumentation