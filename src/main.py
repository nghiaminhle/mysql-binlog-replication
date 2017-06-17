import time
from apollo.event_publisher import EventPublisher
from apollo.processors.dynamic_throttling import DynamicThrottling
import os
from queue import Queue
from kafka import KafkaConsumer, KafkaProducer
from apollo.configurations import kafka_bootstrap_server, topic_binlog, binlog_consumer_group
from apollo.monitoring.performance_counter import PerformanceCounter
from threading import Thread

def main():
    print("start event publisher")
    event_publisher = EventPublisher()
    event_publisher.start()
    
    while True:
        time.sleep(1)

if __name__ == "__main__":
    main()