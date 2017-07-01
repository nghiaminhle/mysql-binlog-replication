from raven import Client
from apollo.configurations import sentry_dns

class Warning:
    _sentry_client = None

    def __init__(self):
        self._sentry_client = Client(sentry_dns)

    def warn(self, msg):
        print('warning', msg)
        self._sentry_client.captureMessage(msg)