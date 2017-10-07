from threading import Lock
class PerformanceCounter:
    _count = 0
    _name = ""
    _lock_object = None
    
    def __init__(self, name):
        self._name = name
        self._lock_object = Lock()

    def increase(self, value):
        self._lock_object.acquire()
        try:
            self._count = self._count + value
        finally:
            self._lock_object.release()
    
    def get_message_per_seconds(self):
        return self._count
    
    @property
    def count(self):
        return self._count
    
    @property
    def name(self):
        return self._name