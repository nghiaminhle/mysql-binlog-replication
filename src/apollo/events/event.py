class Event:
    _type = None

    def __init__(self, event_type):
        self._type = event_type
    
    @property
    def type(self):
        return self._type