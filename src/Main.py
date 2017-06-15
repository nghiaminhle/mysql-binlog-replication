import time
from Apollo.EventPublisher import EventPublisher
from Apollo.Processors.DynamicThrottling import DynamicThrottling
import os
from queue import Queue

def main():
    print("start event publisher")
    event_publisher = EventPublisher()
    event_publisher.start()
    
    while True:
        time.sleep(1)
if __name__ == "__main__":
    main()