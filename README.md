# apollo-event-publisher

## Requirement
python3.5 and newer

six

python kafka
https://github.com/dpkp/kafka-python

python mysql replication
https://github.com/noplay/python-mysql-replication

## UAT Configurations
```
mysql_settings = {'host': '127.0.0.1', 'port': 3307,'user': 'admin', 'passwd': 'admin'}
kafka_bootstrap_server = 'localhost:9092'
event_table = 'undispatch_events'
schema = 'test'
server_id=123
topic_binlog='binlog'
binlog_consumer_group = 'log_reader'
```

## start 
```
python Main.py
```