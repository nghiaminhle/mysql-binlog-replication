from apollo.mongo_log_processor import MongoLogProcessor
import time

def main():
    mongo_log_processor = MongoLogProcessor()
    mongo_log_processor.start()

    while True:
        time.sleep(1)

if __name__ == "__main__":
    main()