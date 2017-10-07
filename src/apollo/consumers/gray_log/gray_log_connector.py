import logging
import graypy
from apollo.configurations import gray_log_host

class GrayLogConnector:
    _logger = None
    def __init__(self):
        self._logger = logging.getLogger('gray_log')
        self._logger.setLevel(logging.INFO)
        handler = graypy.GELFHandler(gray_log_host, localname="EventPublisher")
        self._logger.addHandler(handler)

    def send(self, message):
        self._logger.info(message)