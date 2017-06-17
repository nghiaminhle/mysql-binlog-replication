from apollo.configurations import kafka_bootstrap_server
from threading import Thread
from kafka import KafkaConsumer
from kafka import TopicPartition
import six

class Consumer:
    
    consumer_thread=None#: Thread
    consumer=None# : KafkaConsumer
    cancel = True

    def __init__(self):
        self.consumer_thread = Thread(target=self.do_consume)
        self.consumer_thread.setDaemon(True)

        topic = 'binlog'
        consumer_group_id = 'log_reader'
        offset_reset = 'earliest'
        
        self.consumer = KafkaConsumer(
            bootstrap_servers=kafka_bootstrap_server, 
            group_id=consumer_group_id, 
            auto_offset_reset = offset_reset,
            enable_auto_commit=True
            )

        self.consumer.subscribe([topic])
        
        #partition = TopicPartition(topic, 0)
        #self.consumer.assign([partition])

        #self.consumer.seek(partition, 274397)
        #o = self.consumer.committed(partition)
        #print(o)

        #self.consumer.seek_to_end()
        #p = self.consumer.position(partition)-1
        #print('last', p)
        #self.consumer.seek(partition, p)
       
        #for message in self.consumer:
        #    print ('consumer', message.partition, message.offset, message.value.decode())
        #    break

        #poll_res = self.consumer.poll(max_records=100)
        
        #for msg in self.consumer.poll(max_records=1):
        #    print(msg)
        
        #for partition, msgs in six.iteritems(poll_res):
        #    print(msgs.value.decode())
        #    break
        
        
    
    def start(self):
        self.cancel = False
        self.consumer_thread.start()
        

    def do_consume(self):
        for message in self.consumer:
            if self.cancel == False:
                print ('consumer', message.partition, message.offset, message.value.decode())
                self.consumer.commit()

    def __del__(self):
        self.cancel = True