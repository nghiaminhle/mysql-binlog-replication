class EventDispatcher:
    _handlers = None

    def __init__(self):
        self._handlers = dict()

    def has_event_handler(self, event_type, handler):
        if event_type in self._handlers.keys():
            return handler in self._handlers[event_type]
        return False

    def add_event_handler(self, event_type, handler):
        if not self.has_event_handler(event_type, handler):
            handlers = self._handlers.get(event_type, [])
            handlers.append(handler)
            self._handlers[event_type] = handlers
    
    def remove_event_handler(self, event_type, handler):
        if self.has_event_handler(event_type, handler):
            handlers = self._handlers[event_type]
            if len(handlers) == 1:
                del self._handlers[event_type]
            else:
                handlers.remove(handler)
                self._handlers[event_type] = handlers
                
    
    def dispatch(self, event):
        if event.type in self._handlers.keys():
            handlers = self._handlers[event.type]
            for handler in handlers:
                handler.handle(event)