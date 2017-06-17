from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent
)
from apollo.configurations import mysql_settings, event_table, server_id, schema
from apollo.log.binlog.reader.binlog_metadata import BinLogMetadata

class BinLogStreamFactory:

    def factory(self, binlog_metadata: BinLogMetadata) -> BinLogStreamReader:
        if binlog_metadata is None:
            return BinLogStreamReader(
                connection_settings=mysql_settings,
                server_id=server_id,
                only_events=[ DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent]
                #,only_tables=[event_table]
            )
        else:
            return BinLogStreamReader(
                connection_settings=mysql_settings,
                server_id=server_id,
                log_file=binlog_metadata.log_file,
                log_pos=binlog_metadata.log_pos,
                resume_stream = True,
                auto_position = False,
                only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent]
                #,only_tables=[event_table]
            )