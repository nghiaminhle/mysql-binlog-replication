import time
from apollo.event_publisher import EventPublisher
import sys
import logging
from apollo.monitoring.log import config_log
from datetime import datetime
from apollo.monitoring.warning import Warning
import sys, getopt

commit_database_enable = True

def main(argv):
    print("Start event publisher")
    config_log_by_args(argv)
    retry_count = 0
    while True:
        try:
            event_publisher = EventPublisher(commit_database_enable)
            event_publisher.start()
        except Exception as exp:
            logger = logging.getLogger('exception')
            logger.exception(exp)
            logger = logging.getLogger('general')
            logger.info('Cannot start event publisher')
            waning_critical()
            break

        if event_publisher.cancellation.is_cancel and event_publisher.cancellation.stopped_forever:
            waning_critical()
            break
        warning_retry(retry_count)
        time.sleep(5)

def config_log_by_args(argv):
    try:
      opts, args = getopt.getopt(argv,"ldf")
    except getopt.GetoptError:
      pass
    log_checkpoint = False
    log_debug = False

    for opt, arg in opts:
        if opt == '-l':
            log_checkpoint = True
        if opt == '-d':
            log_debug = True
        if opt == '-f':
            global commit_database_enable
            commit_database_enable = False

    config_log(log_checkpoint, log_debug)

def waning_critical():
    msg = 'Stop BinLog Listner At {}'.format(datetime.now())
    #warning = Warning()
    #warning.warn(msg)

def warning_retry(retry_count):
    msg = 'Re Connect To Read BinLog Cause By Error. Retry Count {}. At {}'.format(retry_count, datetime.now())
    logger = logging.getLogger('general')
    logger.info(msg)
    #warning = Warning()
    #warning.warn(msg)
    
if __name__ == "__main__":
    main(sys.argv[1:])