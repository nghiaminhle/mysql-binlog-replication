from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent
)

import time

#local
mysql_settings = {'host': '127.0.0.1', 'port': 3307,'user': 'admin', 'passwd': 'admin'}

stream = BinLogStreamReader(
                connection_settings=mysql_settings,
                server_id=121
                ,only_events=[ WriteRowsEvent]
                ,freeze_schema = True
                #,only_tables=['test']
                #,log_file='db-241-bin.001128'
                #,log_pos=724765436
                #,resume_stream = True
                #,auto_position = False
            )
start = time.clock()
count = 0
log_file = ""
log_pos = 0

for binlogevent in stream:
    #binlogevent.dump()
    for row in binlogevent.rows:
        values = row['values']
        count = count + 1
        if count == 1:
            log_pos = stream.log_pos
            log_file = stream.log_file
        #print(stream.log_file, stream.log_pos)
        #if log_pos > 225959427 and log_file=='db-241-bin.001129':
        #    break
    
stream.close()
end = time.clock()
print('end', end-start, count, log_file, log_pos)
