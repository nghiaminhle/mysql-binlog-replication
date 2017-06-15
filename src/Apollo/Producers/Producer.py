from Apollo.Configurations import kafka_bootstrap_server
from Apollo.Processors.Cancellation import Cancellation
from queue import Queue
from threading import Thread
from threading import Lock
from kafka import KafkaProducer
import time
import six
from Apollo.Processors.DynamicThrottling import DynamicThrottling
from Apollo.Producers.PublisherHandler import PublisherHandler
from Apollo.Monitoring.Tracing import handle_producer_exception
from Apollo.Monitoring.Retry import retry

class Producer:
    _listening_thread = None  # : Thread
    _lock_object = None
    _cancellation = None
    _throttling = None
    _pending_events = Queue(maxsize=0)
    _handler = None

    def __init__(self, throttling: DynamicThrottling, peding_event_queue: Queue):
        self._lock_object = Lock()
        self._throttling = throttling
        self._pending_events = peding_event_queue

    def start(self, handler:PublisherHandler, cancellation: Cancellation):
        self._cancellation = cancellation
        self._handler = handler

        self._listening_thread = Thread(target=self._do_listening)
        self._listening_thread.setDaemon(True)
        self._listening_thread.start()

    def stop(self):
        self._lock_object.acquire()
        try:
            self._cancellation.cancel()
        finally:
            self._lock_object.release()

    def _do_listening(self):
        while True and not self._cancellation.is_cancel:
            if not self._pending_events.empty():
                envelope = self._pending_events.get()
                retry(self._send, self._handle_send_exception, self.stop, envelope)
            else:
                time.sleep(0.1)
        print('stop kafka producer')
        
    def _send(self, envelope):
        self._handler.handle(envelope)
        self._throttling.wait_to_publish(self._cancellation)
    
    def _handle_send_exception(self, exp):
        handle_producer_exception(exp)
        self._throttling.penalize_publish_error()