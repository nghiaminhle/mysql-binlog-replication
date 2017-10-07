from threading import Thread
import time
from apollo.monitoring.instrumentation import Instrumentation
from apollo.processors.dynamic_throttling import DynamicThrottling
from apollo.configurations import limitation_pending_queue
from .performance_reporter import PerformanceReporter
from .performance_metric import PerformanceMetric
from apollo.log.binlog.commit.checkpoint_committer import CheckPointCommitter
from apollo.processors.cancellation import Cancellation

class Monitor:
    _monitored_queues = None
    _monitoring_thread = None
    _cancel = False
    _instrumentation = None

    CONST_PENDING_QUEUE = 'pending_queue'
    CONST_COMMIT_QUEUE = 'commit_queue'
    LIMITATION_PENDING_QUEUE = limitation_pending_queue
    
    _throttling = None
    _reporter = None
    _commiter = None
    _cancellation = None

    def __init__(self, pending_queue, publish_queue, throttling, reporter, cancellation: Cancellation):
        self._monitored_queues = dict()
        self._monitored_queues[self.CONST_PENDING_QUEUE] = pending_queue
        self._monitored_queues[self.CONST_COMMIT_QUEUE] = publish_queue
        self._instrumentation = Instrumentation()
        
        self._monitoring_thread = Thread(target=self._do_monitor)
        self._monitoring_thread.setDaemon(True)
        self._throttling = throttling
        self._reporter = reporter
        self._cancellation = cancellation

    def start(self):
        self._monitoring_thread.start()

    def stop(self):
        self._cancel = True    
    
    def monitor_commiter(self, commiter: CheckPointCommitter):
        self._commiter = commiter

    _total_read_log = 0
    _total_filter = 0
    _total_publish = 0
    _total_commit = 0

    def _do_monitor(self):
        count = 0
        while True and not self._cancellation.is_cancel:
            self.report()
            count = count + 1
            #print('seconds', count)
            self._manage_queue()
            time.sleep(1)
    
    def _manage_queue(self):
        if self._monitored_queues[self.CONST_PENDING_QUEUE].qsize() > self.LIMITATION_PENDING_QUEUE:
            self._throttling.throttle_read()
    
    def report(self):
        metric = self._get_performane_metric()
        self._reporter.report(metric)
    
    def _get_performane_metric(self):
        read_log_per_seconds = self._instrumentation.read_log_counter.count - self._total_read_log
        self._total_read_log = self._instrumentation.read_log_counter.count
        
        filter_per_seconds = self._instrumentation.filter_counter.count - self._total_filter
        self._total_filter = self._instrumentation.filter_counter.count

        pubish_per_seconds = self._instrumentation.publish_kafka_counter.count - self._total_publish
        self._total_publish = self._instrumentation.publish_kafka_counter.count

        commit_per_seconds = self._instrumentation.commit_counter.count - self._total_commit
        self._total_commit = self._instrumentation.commit_counter.count

        metric = PerformanceMetric()
        metric.read_log_count = self._total_read_log
        metric.read_log_per_seconds = read_log_per_seconds
        
        metric.filter_log_count = self._total_filter
        metric.filter_log_per_seconds = filter_per_seconds

        metric.publish_count = self._total_publish
        metric.publish_per_seconds = pubish_per_seconds

        metric.commit_count = self._total_commit
        metric.commit_count_per_seconds = commit_per_seconds

        metric.pending_queue_count =  self._monitored_queues[self.CONST_PENDING_QUEUE].qsize()
        metric.commit_queue_count = self._monitored_queues[self.CONST_COMMIT_QUEUE].qsize()

        metric.holing_checkpoint_counts = self._commiter.holding_checkpoint_counts()
        metric.waiting_checkpoint_counts = self._commiter.waiting_checkpoint_counts()

        return metric
    
    def get_instrumentation(self):
        return self._instrumentation