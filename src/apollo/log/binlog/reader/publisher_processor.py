from threading import Thread
from apollo.processors.cancellation import Cancellation
from apollo.monitoring.instrumentation import Instrumentation
from apollo.log.binlog.commit.checkpoint_committer import CheckPointCommitter
from .kafka_publisher import KafkaPublisher
from apollo.processors.processor import Processor
from queue import Queue
from apollo.processors.cancellation import Cancellation
from apollo.log.binlog.reader.binlog_metadata import BinLogMetadata
import time
from apollo.configurations import min_pending_queue, limitation_pending_queue, mysql_settings, schema, event_table
from datetime import datetime
import logging
import pymysql.cursors

connection = pymysql.connect(host=mysql_settings['host'],
                             port=mysql_settings['port'],
                             user=mysql_settings['user'],
                             password=mysql_settings['passwd'],
                             db=schema,
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

class PublisherProccessor(Processor):
    _publisher_thread = None
    _kafka_publisher = None
    _instrumentation = None
    _cancellation = None
    _pending_events = None
    _checkpoint_commiter = None
    _commit_database_enable = False

    def __init__(self, 
                pending_events: Queue, 
                kafka_publisher: KafkaPublisher, 
                commiter: CheckPointCommitter, 
                instrumentation: Instrumentation, 
                commit_database_enable: bool):
        self._pending_events = pending_events
        self._publisher_thread = Thread(target=self.publish)
        self._publisher_thread.setDaemon(True)
        self._kafka_publisher = kafka_publisher
        self._instrumentation = instrumentation
        self._checkpoint_commiter = commiter
        self._commit_database_enable = commit_database_enable

    def start(self, cancellation: Cancellation):
        self._cancellation = cancellation
        self._publisher_thread.start()
    
    def stop(self):
        self._cancellation.cancel()

    def publish(self):
        threshold = 0.0001
        delay = threshold
        while True and not self._cancellation.is_cancel:
            if not self._pending_events.empty():
                envelope = self._pending_events.get()
                try:
                    self._kafka_publisher.handle(envelope, self._publish_successful_callback, self._publish_fail_callback)
                except Exception as exp:
                    logger = logging.getLogger('exception')
                    logger.exception(exp)
                    self.stop()

                if delay > 0:
                    time.sleep(delay)
                if self._pending_events.qsize() > limitation_pending_queue:
                    delay = 0
                if self._pending_events.qsize() < min_pending_queue:
                    delay = threshold
            else:
                #print('kafka sleee')
                time.sleep(0.1)
    
    def _publish_successful_callback(self, metadata, checkpoint: BinLogMetadata):
        self._checkpoint_commiter.release_checkpoint(checkpoint)
        self._instrumentation.publish_to_kafka()
        self._update_event_store(checkpoint)

    def _publish_fail_callback(self, metadata, checkpoint: BinLogMetadata):
        msg = "Publish Kafka Fail {} {}".format(datetime.now(), checkpoint.key)
        logger = logging.getLogger('general')
        logger.debug(msg)
        self.stop()
    
    def _update_event_store(self, checkpoint: BinLogMetadata):
        if not self._commit_database_enable:
            return
        if checkpoint.schema!=schema or checkpoint.table!=event_table:
            return
        try:
            id = checkpoint.row_id
            with connection.cursor() as cursor:
                sql = 'update ' + event_table + ' set status = 2 where id=%s'
                cursor.execute(sql, (id,))
                connection.commit()
        except Exception as exp:
            logger = logging.getLogger('exception')
            logger.exception(exp)
            self.stop()


    def join(self):
        self._publisher_thread.join()