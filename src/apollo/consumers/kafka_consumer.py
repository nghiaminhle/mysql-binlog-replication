from apollo.configurations import kafka_bootstrap_server
from threading import Thread
from kafka import KafkaConsumer
from kafka import TopicPartition
from apollo.monitoring.retry import retry
import six

class Consumer:
    _consumer_thread = None #: Thread
    _consumer = None #: KafkaConsumer
    _cancel = True
    _auto_commit = False
    _message_handler = None


    def __init__(self, topics, consumer_group, offset_reset = 'earliest', auto_commit = False):
        self._consumer_thread = Thread(target=self.do_consume)
        self._consumer_thread.setDaemon(True)

        self._auto_commit = auto_commit
        self._consumer = KafkaConsumer(
                bootstrap_servers=kafka_bootstrap_server, 
                group_id=consumer_group, 
                auto_offset_reset = offset_reset,
                enable_auto_commit= auto_commit
            )

        self._consumer.subscribe(topics)
    
    def start(self, message_handler):
        self._cancel = False
        self._consumer_thread.start()
        self._message_handler = message_handler
    
    def stop(self):
        self._cancel = True
        
    def do_consume(self):
        for message in self._consumer:
            if self._cancel == False:
                retry(self._process_message,self._handle_exception, self._handle_exceeded_retry, message)
    
        print ("stop consumer")

    def _process_message(self, message):
        self._message_handler.handle(message)
        if not self._auto_commit:
            self._consumer.commit_async()
    
    def _handle_exception(self, exp):
        print(exp)
    
    def _handle_exceeded_retry(self):
        print("retry exceeded")
        self.stop

    def __del__(self):
        self._cancel = True