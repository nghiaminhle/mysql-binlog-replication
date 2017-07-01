from threading import Lock

class Cancellation:
    _cancel = False
    _lock = None

    def __init__(self):
        self._lock = Lock()

    def cancel(self, state = True):
        self._lock.acquire()
        try:
            self._cancel = state
        finally:
            self._lock.release()
    
    @property
    def is_cancel(self):
        return self._cancel