from .binlog_condition import BinLogCondition
from apollo.configurations import filtered_conditions
from .row_condition import RowCondition, InsertCondition, UpdateCondition, DeleteCondition
from .eventstore_condition import EventStoreCondition
from .constants import INSERT_EVENT, UPDATE_EVENT, DELETE_EVENT

class ConditionConfiguration:
    # retrun BinLogCondition[]
    def load(self):
        conditions = []
        return conditions
    
class DefaultConditionConfiguration(ConditionConfiguration):
    # retrun BinLogCondition[]
    def load(self):
        conditions = []
        event_store_condition = EventStoreCondition()
        conditions.append(event_store_condition)
        for configurations in filtered_conditions:
            condition = self._get_row_condition(configurations)
            conditions.append(condition)
        
        return conditions
    
    def _get_row_condition(self, configurations):
        event_type = configurations['event_type']
        condition = None
        if event_type == INSERT_EVENT:
            condition = InsertCondition(configurations['schema'], configurations['table'], configurations['topic'])
        elif event_type == UPDATE_EVENT:
            changed_columns = configurations['changed_columns'] if 'changed_columns' in configurations.keys() else None
            filtered_columns = configurations['filtered_columns'] if 'filtered_columns' in configurations.keys() else None
            condition = UpdateCondition(configurations['schema'], configurations['table'], configurations['topic'], changed_columns, filtered_columns)
        elif event_type == DELETE_EVENT:
            condition = DeleteCondition(configurations['schema'], configurations['table'], configurations['topic'])
        
        return condition