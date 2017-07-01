class Message:
    row_event_type = None
    schema = None
    table = None
    primary_key = None

    def __init__(self, row_event_type, schema, table, primary_key):
        self.row_event_type = row_event_type
        self.schema = schema
        self.table = table
        self.primary_key = primary_key
    
    def to_dict(self):
        return {'row_event_type': self.row_event_type, 'schema': self.schema, 'table': self.table, 'primary_key': self.primary_key}
    