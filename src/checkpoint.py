from apollo.processors.processor import Processor
from apollo.consumers.kafka_consumer import Consumer
from apollo.consumers.binlog.binlog_handler import BinLogHandler
from apollo.configurations import topic_binlog
import time

def main():
    handler = BinLogHandler()
    consumer = Consumer([topic_binlog], 'checkpoint1', offset_reset = 'latest')
    consumer.start(handler) 

    while True:
        time.sleep(1)

if __name__ == "__main__":
    main()