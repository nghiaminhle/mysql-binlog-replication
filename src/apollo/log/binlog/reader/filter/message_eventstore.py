from .message import Message
import json
import logging

# message for table event store 2
class MessageEventStore(Message):
    row_vals = None

    def __init__(self, row_event_type, schema, table, primary_key, row_vals):
        Message.__init__(self, row_event_type, schema, table, primary_key)
        self.row_vals = row_vals

    def to_dict(self):
        body = dict()
        body['message_id'] = self.row_vals['message_id']
        body['event_type'] = self.row_vals['type']
        body['source'] = self.row_vals['source']
        body['created_at'] = self.row_vals['created_at']
        try:
            body['payload'] = json.loads(self.row_vals['payload'])
        except Exception as exp:
            logger = logging.getLogger('exception')
            logger.exception(exp)
            body['payload'] = self.row_vals['payload']

        body['request_id'] = self.row_vals['message_id']
        body['request_time'] = self.row_vals['created_at']
        
        return body