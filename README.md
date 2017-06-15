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
mysql_settings = {'host': '10.20.43.11', 'port': 3306,'user': 'talaria_binlog', 'passwd': 'N8hz78CqzRUyqCHW'}
kafka_bootstrap_server = '10.20.43.25:9092'
event_table = 'event_store2'
schema = 'tala_migration'
server_id=243
topic_binlog='binlog_uat'
binlog_consumer_group = 'log_reader'
```

## start 
```
python Main.py
```