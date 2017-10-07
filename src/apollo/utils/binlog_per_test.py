from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent
)
from threading import Thread
import time
from kafka import KafkaProducer
from queue import Queue

kafka_bootstrap_server = 'localhost:9092'

#local
mysql_settings = {'host': '127.0.0.1', 'port': 3307,'user': 'admin', 'passwd': 'admin'}

#uat
#mysql_settings = {'host': '10.20.43.11', 'port': 3306,'user': 'talaria_binlog', 'passwd': 'N8hz78CqzRUyqCHW'}

#production
#mysql_settings = {'host': '10.20.40.243', 'port': 3306,'user': 'integration_binlog', 'passwd': 'test'}

pending_events = Queue(maxsize=0)
msg = '{"created_at": "2017-01-01 00:00:00", "destination": "to cpn", "event_type": "update", "message_id": "abee3273-bcbd-4323-9e8c-3131fadcd4e8", "object_id": "55400725", "payload": "{\"action\":\"update\",\"id\":\"\",\"code\":\"5540075\",\"status\":\"cho_in\",\"last_status\":\"cho_in\"}", "source": "from bop"}'

def test_binlog():
    stream = BinLogStreamReader(
                connection_settings=mysql_settings,
                server_id=121
                ,only_events=[ WriteRowsEvent]
                ,freeze_schema = True
                #,only_tables=['event_store']
                ,log_file='0b74efb26d3c-bin.000009'
                ,log_pos=32432136
                ,resume_stream = True
                ,auto_position = False
            )
    start = time.clock()        
    count = 0
    msg_batchs = []
    for binlogevent in stream:
        #binlogevent.dump()
        for row in binlogevent.rows:
            values = row['values']
            count = count + 1
            msg_batchs.append(msg)
            if len(msg_batchs) == 10000:                
                pending_events.put(msg_batchs)
                msg_batchs = []
            
    stream.close()
    end = time.clock()
    execution_time = end - start
    print('binlog end:', end-start, count, 'throughput:', count/execution_time, "\n\n")

kafka_number = 0
def test_kafka(counter):
    producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_server)
    topic = 'test'
    sleep_count = 0
    while True:
        if not pending_events.empty():
            messages = pending_events.get()
            for message in messages:
                producer.send(topic, str.encode(message))
                counter.count()
        else:
            sleep_count = sleep_count + 1
            #print('kafka sleee', sleep_count)
            time.sleep(0.1)

class Counter:
    kafka_count = 0

    def count(self):
        self.kafka_count = self.kafka_count + 1

def main():
    binlog_thread = Thread(target=test_binlog)
    binlog_thread.setDaemon(True)
    counter = Counter()
    kafka_thread = Thread(target=test_kafka, args=(counter,))
    kafka_thread.setDaemon(True)

    binlog_thread.start()
    kafka_thread.start()

    #binlog_thread.join()
    #kafka_thread.join()
    published_number = 0
    count = 0
    while True:
        count = count + 1
        print('kafka', count, counter.kafka_count, 'throughput', counter.kafka_count-published_number)
        published_number = counter.kafka_count
        time.sleep(1)

if __name__ == "__main__":
    main()