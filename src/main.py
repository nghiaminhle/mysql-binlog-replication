import time
from apollo.event_publisher import EventPublisher

def main():
    print("start event publisher")
    event_publisher = EventPublisher()
    event_publisher.start()
    
    while True:
        time.sleep(1)

if __name__ == "__main__":
    main()