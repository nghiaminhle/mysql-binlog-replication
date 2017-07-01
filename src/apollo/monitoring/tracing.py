import sys
# enable print_log_reader 
enable_print_log_reader = True
# enable print_publish_message
enable_print_publish_message = False
# enable print commit message
enable_print_commit_message = False

def print_log_reader(message):
    if enable_print_log_reader:
        print(message)

def print_publish_message(message):
    if enable_print_publish_message:
        print(message)

def print_commit_message(message):
    if enable_print_commit_message:
        print(message)

def handle_log_reader_exception(exp):
    print ("Log Reader Unexpected error:", sys.exc_info(), exp)

def handle_commit_exception(exp):
    print ("Kafka Commit Unexpected error:", sys.exc_info(), exp)

def handle_producer_exception(exp):
    print ("Kafka producer Unexpected error:", sys.exc_info(), exp)