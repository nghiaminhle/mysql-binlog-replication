from threading import Lock

class Cancellation:
    _cancel = False
    _lock = None
    _stopped_forever = False

    def __init__(self):
        self._lock = Lock()

    def cancel(self, state = True, stopped_forever = False):
        self._lock.acquire()
        try:
            self._cancel = state
            self._stopped_forever = stopped_forever
        finally:
            self._lock.release()
    
    @property
    def is_cancel(self):
        return self._cancel

    @property
    def stopped_forever(self):
        return self._stopped_forever