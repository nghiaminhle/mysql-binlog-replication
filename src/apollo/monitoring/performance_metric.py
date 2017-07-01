class PerformanceMetric:
    _pending_queue_count = 0
    _commit_queue_count = 0
    _read_log_count = 0
    _read_log_per_seconds = 0
    _filter_log_count = 0
    _filter_log_per_seconds = 0
    _publish_count = 0
    _publish_per_seconds = 0
    _commit_count = 0
    _commit_count_per_seconds = 0
    _reported_at = ''

    @property
    def pending_queue_count(self):
        return self._pending_queue_count
    
    @pending_queue_count.setter
    def pending_queue_count(self, value):
        self._pending_queue_count = value
    
    @property
    def commit_queue_count(self):
        return self._commit_queue_count
    
    @commit_queue_count.setter
    def commit_queue_count(self, value):
        self._commit_queue_count = value
    
    @property
    def read_log_count(self):
        return self._read_log_count
    
    @read_log_count.setter
    def read_log_count(self, value):
        self._read_log_count = value
    
    @property
    def read_log_per_seconds(self):
        return self._read_log_per_seconds
    
    @read_log_per_seconds.setter
    def read_log_per_seconds(self, value):
        self._read_log_per_seconds = value
    
    @property
    def filter_log_count(self):
        return self._filter_log_count
    
    @filter_log_count.setter
    def filter_log_count(self, value):
        self._filter_log_count = value
    
    @property
    def filter_log_per_seconds(self):
        return self._filter_log_per_seconds
    
    @filter_log_per_seconds.setter
    def filter_log_per_seconds(self, value):
        self._filter_log_per_seconds = value
        
    @property
    def publish_count(self):
        return self._publish_count
    
    @publish_count.setter
    def publish_count(self, value):
        self._publish_count = value
    
    @property
    def publish_per_seconds(self):
        return self._publish_per_seconds
    
    @publish_per_seconds.setter
    def publish_per_seconds(self, value):
        self._publish_per_seconds = value
    
    @property
    def commit_count(self):
        return self._commit_count
    
    @commit_count.setter
    def commit_count(self, value):
        self._commit_count = value
    
    @property
    def commit_count_per_seconds(self):
        return self._commit_count_per_seconds
    
    @commit_count_per_seconds.setter
    def commit_count_per_seconds(self, value):
        self._commit_count_per_seconds = value
    
    @property
    def reported_at(self):
        return self._reported_at
    
    @reported_at.setter
    def reported_at(self, value):
        self.reported_at = value