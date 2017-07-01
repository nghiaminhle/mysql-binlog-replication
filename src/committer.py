from apollo.log.binlog.reader.binlog_metadata import BinLogMetadata
from apollo.log.binlog.commit.checkpoint_committer import CheckPointCommitter
import time

checkpoint1 = BinLogMetadata(100,'test.123')
checkpoint2 = BinLogMetadata(101,'test.123')
checkpoint3 = BinLogMetadata(102,'test.123')
committer = CheckPointCommitter()
committer.start()

committer.commit_checkpoint(checkpoint2)
committer.hold_checkpoint(checkpoint1)
committer.commit_checkpoint(checkpoint3)
count = 0
while True:
    count = count + 1
    print(count)
    if count == 3:
        committer.release_checkpoint(checkpoint1)
    
    if count == 4:
        committer.commit_checkpoint(checkpoint1)
        committer.hold_checkpoint(checkpoint3)
        committer.hold_checkpoint(checkpoint2)
    
    if count == 6:
        committer.release_checkpoint(checkpoint3)
    
    if count == 7:
        committer.release_checkpoint(checkpoint2)
    time.sleep(1)