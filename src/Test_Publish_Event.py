import pymysql.cursors
import time
from Apollo.Configurations import mysql_settings, schema, event_table
import uuid

def main():
    connection = pymysql.connect(host=mysql_settings['host'],
                             port=mysql_settings['port'],
                             user=mysql_settings['user'],
                             password=mysql_settings['passwd'],
                             db=schema,
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)
    count = 0
    for i in range(0,10):
        count = count + 1
        
        with connection.cursor() as cursor:
                # Create a new record
                msg_id = str(uuid.uuid4())
                topic = 'backorder'
                table = event_table #event_table
                print(msg_id)
                sql = "INSERT INTO `"+schema+"`.`"+table+"`"
                sql = sql +'''
                         (`message_id`,`routing_key`,`created_at`,`updated_at`,`type`,`object_id`,`source`,`destination`,`topic`,`status`,`retry_times`,`payload`,`error`) VALUES (%s,'test','2017-01-01 00:00:00','2017-01-01 00:00:00','update','55400725','from bop','to cpn',% s,1,1,'{"action":"update","id":"","code":"5540075","status":"cho_in","last_status":"cho_in"}','test');
                    '''
                cursor.execute(sql, (msg_id,topic,))

                # connection is not autocommit by default. So you must commit to save
                # your changes.
                connection.commit()
        print("insert", table, count)
        time.sleep(0.1)

if __name__ == "__main__":
    main()