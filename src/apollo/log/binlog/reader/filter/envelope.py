from apollo.log.log_metadata import LogMetadata
from .message import Message

class Envelope:
    # time read from log
    _created_at = None
    # time sent to kafka
    _sent_at = None
    # Message
    body = None
    # LogMetadata
    log_metadata = None

    def __init__(self, body, log_metadata):
        self.body = body
        self.log_metadata = log_metadata