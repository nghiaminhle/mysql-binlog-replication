
# localhost setting
mysql_settings = {'host': '127.0.0.1', 'port': 3307,'user': 'admin', 'passwd': 'admin'}
kafka_bootstrap_server = 'localhost:9092'
event_table = 'event_store2'
schema = 'tiki_web'
server_id=123
topic_binlog='binlog'
binlog_consumer_group = 'log_reader'

# uat setting
#mysql_settings = {'host': '10.20.43.11', 'port': 3306,'user': 'talaria_binlog', 'passwd': 'N8hz78CqzRUyqCHW'}
#kafka_bootstrap_server = '10.20.43.25:9092'
#event_table = 'event_store2'
#schema = 'tala_migration'
#server_id=313
#topic_binlog='binlog_uat'
#binlog_consumer_group = 'log_reader'

#monitor configurations
max_retry_count = 3
time_sleep_pubish_error = 1 # in seconds