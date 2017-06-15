
# localhost setting
mysql_settings = {'host': '127.0.0.1', 'port': 3307,'user': 'admin', 'passwd': 'admin'}
kafka_bootstrap_server = 'localhost:9092'
event_table = 'undispatched_events'
schema = 'test'
server_id=123
topic_binlog='binlog'
binlog_consumer_group = 'log_reader'

#monitor configurations
max_retry_count = 3
time_sleep_pubish_error = 1 # in seconds