# apollo-event-publisher

## Requirement
python3.5 and newer

**six**

pip install six

**python kafka**

pip install kafka-python

https://github.com/dpkp/kafka-python

**python mysql replication**

pip install mysql-replication

https://github.com/noplay/python-mysql-replication

**python mongo**

pip install pymongo

https://github.com/mongodb/mongo-python-driver 

**graypy - gray log**

pip install graypy

https://github.com/severb/graypy

**raven - sentry**

pip install raven

https://docs.sentry.io/clients/python/

## Configurations

/apollo/configurations.py

```
mysql_settings = {'host': '127.0.0.1', 'port': 3307,'user': 'admin', 'passwd': 'admin'}
kafka_bootstrap_server = 'localhost:9092'
event_table = 'undispatch_events'
schema = 'test'
server_id=123
topic_binlog='binlog'
binlog_consumer_group = 'log_reader'
```

### Undispatched event

```
CREATE TABLE `undispatched_events` (
  `id` bigint(11) unsigned NOT NULL AUTO_INCREMENT,
  `message_id` varchar(36) NOT NULL,
  `routing_key` varchar(50) DEFAULT NULL,
  `created_at` datetime DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime DEFAULT CURRENT_TIMESTAMP,
  `type` varchar(50) DEFAULT NULL,
  `object_id` varchar(50) DEFAULT NULL,
  `source` varchar(45) DEFAULT NULL,
  `destination` varchar(50) DEFAULT NULL,
  `topic` varchar(255) NOT NULL,
  `status` tinyint(1) DEFAULT '0' COMMENT '0=New | 1=RUNNING | 2:SUCCESS | -1:FAILURE',
  `retry_times` tinyint(4) DEFAULT '0',
  `payload` text,
  `error` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=396 DEFAULT CHARSET=utf8;

```

## start 
```
python binlog_listener.py
```