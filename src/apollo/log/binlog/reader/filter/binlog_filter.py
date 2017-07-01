from .binlog_condition import BinLogCondition
from .eventstore_condition import EventStoreCondition
from .row_condition import RowCondition
from .constants import UPDATE_EVENT, DELETE_EVENT
from .binlog_condition_config import ConditionConfiguration

class BinLogFilter:
    _conditions = [] # BinLogCondition

    def __init__(self, condition_configuration: ConditionConfiguration):
        self._conditions = condition_configuration.load()
    
    # return list kafka envelopes
    def filter(self, binlogevent, row, log_pos, log_file):
        envelopes = []
        for condition in self._conditions:
            if condition.is_satisfy(binlogevent, row):
                envelope = condition.build_envelope(binlogevent, row, log_pos, log_file)
                envelopes.append(envelope)

        return envelopes