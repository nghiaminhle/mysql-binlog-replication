from apollo.log.binlog.reader.binlog_metadata import BinLogMetadata
from apollo.log.binlog.commit.commit_handler import CommitHandler

checkpoint = BinLogMetadata(575722679,'uat-tala-db-11-bin.001927')
handler = CommitHandler()

handler.handle(checkpoint)

print('complete')