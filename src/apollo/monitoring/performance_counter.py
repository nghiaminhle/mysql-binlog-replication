class PerformanceCounter:
    _count = 0
    _name = ""
    
    def __init__(self, name):
        self._name = name

    def increase(self, value):
        self._count = self._count + value
    
    def get_message_per_seconds(self):
        return self._count
    
    @property
    def count(self):
        return self._count
    
    @property
    def name(self):
        return self._name