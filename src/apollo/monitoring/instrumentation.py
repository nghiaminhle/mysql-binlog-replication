from apollo.monitoring.performance_counter import PerformanceCounter

class Instrumentation:
    _read_log_counter = None
    _filter_counter = None
    _publish_kafka_counter = None
    _commit_counter = None

    def __init__(self):
        self._read_log_counter = PerformanceCounter("Read Log")
        self._filter_counter = PerformanceCounter("Filter Log")
        self._publish_kafka_counter = PerformanceCounter("Publish To Kafka")
        self._commit_counter = PerformanceCounter("Commit")

    def read_log(self):
        self._read_log_counter.increase(1)
        
    def filter(self):
        self._filter_counter.increase(1)

    def publish_to_kafka(self):
        self._publish_kafka_counter.increase(1)

    def commit_to_kafka(self):
        self._commit_counter.increase(1)

    @property
    def read_log_counter(self):
        return self._read_log_counter
    
    @property
    def filter_counter(self):
        return self._filter_counter
    
    @property
    def publish_kafka_counter(self):
        return self._publish_kafka_counter
    
    @property
    def commit_counter(self):
        return self._commit_counter