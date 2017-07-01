from apollo.configurations import kafka_bootstrap_server
from kafka import KafkaConsumer, TopicPartition
from kafka.client_async import KafkaClient
from kafka.cluster import ClusterMetadata

topic = 'backorder'
consumer_group = 'graylog1'
offset_reset = 'earliest'
auto_commit = False
consumer = KafkaConsumer(
                bootstrap_servers=kafka_bootstrap_server, 
                group_id=consumer_group, 
                auto_offset_reset = offset_reset,
                enable_auto_commit= auto_commit
            )

def get_last_commited_offset():
    partition = TopicPartition(topic, 0)
    consumer.assign([partition])
    last_committed_offset = consumer.committed(partition)
    print("commited", last_committed_offset)

def get_last_offset():
    partition = TopicPartition(topic, 0)
    consumer.assign([partition])
    consumer.seek_to_end()
    end = consumer.position(partition)
    print("last", end)


def test_kafka_client():
    kafka_client = KafkaClient(bootstrap_servers=kafka_bootstrap_server, api_version=(0, 10))
    metadata = kafka_client.cluster
    partitions = metadata.available_partitions_for_topic("inventory2")
    print(partitions)

    topics = metadata.topics()
    for t in topics:
        print(t)
    print('topic', topics)

def high_water():
    partition = TopicPartition(topic, 0)
    consumer.assign([partition])
    last_committed_offset = consumer.highwater(partition)
    print("high_water", last_committed_offset)

#get_last_commited_offset()
#get_last_offset()

test_kafka_client()
#high_water()