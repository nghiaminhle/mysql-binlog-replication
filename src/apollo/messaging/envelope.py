from apollo.log.log_metadata import LogMetadata
from apollo.messaging.message import Message

class Envelope:
    # time read from log
    _created_at = None
    # time sent to kafka
    _sent_at = None
    # Message
    _body = None
    # LogMetadata
    _log_metadata = None

    def __init__(self, body, log_metadata):
        self._body = body
        self._log_metadata = log_metadata
    
    @property
    def created_at(self):
        return self._created_at
    
    @created_at.setter
    def created_at(self, value):
        self._created_at = value
    
    @property
    def sent_at(self):
        return self.sent_at
    
    @sent_at.setter
    def sent_at(self, value):
        self._sent_at = value
    
    @property
    def body(self):
        return self._body
    
    @body.setter
    def body(self, value):
        self._body = value

    @property
    def log_metadata(self):
        return self._log_metadata
    
    @log_metadata.setter
    def log_metadata(self, value):
        self._log_metadata = value