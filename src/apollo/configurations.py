# production seting
mysql_settings = {'host': '', 'port': 3306,'user': '', 'passwd': ''}
kafka_bootstrap_server = ''
event_table = 'event_table'
schema = 'test_db'
server_id=313
topic_binlog='integration_binlog'
binlog_consumer_group = 'integration_log_reader'

# monitor configurations
max_retry_count = 3
time_sleep_pubish_error = 1 # in seconds
limitation_pending_queue = 1000 # max pending queue number
min_pending_queue = 100 # limitation for recover normally
monitored_topics = []

# limitation for commit last bin log check point
limit_pending_checkpoint_number = 100
# interval auto commit check point in seconds
limit_commit_check_point_time = 5
# time waiting kafka callback in seconds
waiting_callback_time = 10

# condition for filtering
filtered_conditions = []