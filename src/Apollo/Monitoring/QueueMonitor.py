from threading import Thread
import time

class QueueMonitor:
    _monitored_queues = None
    _monitoring_thread = None
    _cancel = False

    CONST_PENDING_QUEUE = 'pending_queue'
    CONST_PUBLISH_QUEUE = 'publish_queue'

    def __init__(self, pending_queue, publish_queue):
        self._monitored_queues = dict()
        self._monitored_queues[self.CONST_PENDING_QUEUE] = pending_queue
        self._monitored_queues[self.CONST_PUBLISH_QUEUE] = publish_queue
        
        self._monitoring_thread = Thread(target=self._do_monitor)
        self._monitoring_thread.setDaemon(True)

    def start(self):
        self._monitoring_thread.start()
        return

    def stop(self):
        self._cancel = True    
        return
    
    def _do_monitor(self):
        while True and not self._cancel:
            print("------------Monitor------------\n Pending Queue:{} \n Commit Queue:{} \n-------------------------------".format(self._monitored_queues[self.CONST_PENDING_QUEUE].qsize(), self._monitored_queues[self.CONST_PUBLISH_QUEUE].qsize()))
           
            time.sleep(1)