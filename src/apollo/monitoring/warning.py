from raven import Client
from apollo.configurations import sentry_dns
import logging

class Warning:

    def warn(self, msg):
        try:
            sentry_client = Client(sentry_dns)
            sentry_client.captureMessage(msg)
        except Exception as exp:
            logger = logging.getLogger('exception')
            logger.exception(exp)