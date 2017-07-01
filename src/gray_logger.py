from apollo.gray_log_processor import GrayLogProcessor
import time

def main():
    gray_log_processor = GrayLogProcessor()
    gray_log_processor.start()

    while True:
        time.sleep(1)

if __name__ == "__main__":
    main()