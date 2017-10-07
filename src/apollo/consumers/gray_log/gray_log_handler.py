from ..message_handler import MessageHandler
from .gray_log_connector import GrayLogConnector

class GrayLogHanlder(MessageHandler):
    _graylog_connector = None

    def __init__(self):
        self._graylog_connector = GrayLogConnector()

    def handle(self, message):
        body = message.value.decode()
        print("gray log", body)
        self._graylog_connector.send(body)