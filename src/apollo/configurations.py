
# localhost setting
mysql_settings = {'host': '127.0.0.1', 'port': 3307,'user': 'admin', 'passwd': 'admin'}
kafka_bootstrap_server = 'localhost:9092'
event_table = 'undispatched_events'
schema = 'test_db'
server_id=123
topic_binlog='binlog'
binlog_consumer_group = 'log_reader'

#monitor configurations
max_retry_count = 3
time_sleep_pubish_error = 1 # in seconds
limitation_pending_queue = 10000
monitored_topics = ['backorder']

#mongo setting 
mongo_setting = {'host': 'localhost', 'port': 28017, 'database': 'integration'}
#gray log host
gray_log_host = 'test'
#sentry for warning
sentry_dns = 'test'
#limitation for commit last bin log check point
limit_pending_checkpoint_number = 100
#interval auto commit check point in seconds
limit_commit_check_point_time = 5 

#condition for filtering
filtered_conditions = [
    {'schema': 'test', 'table':'product', 'event_type': 'update', 'topic':'product'},
    {'schema': 'test', 'table':'person', 'event_type': 'update', 'topic':'backorder', 'changed_columns':'name,value', 'filtered_columns':'name,value'},
    {'schema': 'test', 'table':'product', 'event_type': 'update', 'topic':'product_review', 'changed_columns':'reviews_count,rating_summary,rating_point', 'filtered_columns':'reviews_count,rating_summary,rating_point'}]