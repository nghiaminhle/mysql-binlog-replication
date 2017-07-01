from .message import Message

class InsertedMessage(Message):
    row_vals = None

    def __init__(self, row_event_type, schema, table, primary_key, row_vals):
        Message.__init__(self, row_event_type, schema, table, primary_key, row_vals)
        self.row_vals = row_vals
    
    def to_dict(self):
        body = super().to_dict()
        body['row_vals'] = self.row_vals
        return body
    
    def get_routing_key(self):
        if self.primary_key != None:
            if isinstance(self.primary_key, tuple):
                values = []
                for key in self.primary_key:                    
                    values.append(row_vals[key])
                return ",".join(values)
            elif isinstance(self.primary_key, str):
                return str(self.row_vals[self.primary_key])
        return None
        

class DeletedMessage(Message):
    row_vals = None

    def __init__(self, row_event_type, schema, table, primary_key, row_vals):
        Message.__init__(self, row_event_type, schema, table, primary_key)
        self.row_vals = row_vals
    
    def to_dict(self):
        body = super().to_dict()
        body['row_vals'] = self.row_vals
        return body
    
    def get_routing_key(self):
        if self.primary_key != None:
            if isinstance(self.primary_key, tuple):
                values = []
                for key in self.primary_key:                    
                    values.append(self.row_vals[key])
                return ",".join(values)
            elif isinstance(self.primary_key, str):
                return str(self.row_vals[self.primary_key])
    

class UpdatedMessage(Message):
    before_row_vals = None
    after_row_vals = None

    def __init__(self, row_event_type, schema, table, primary_key, before_row_vals, after_row_vals):
        Message.__init__(self, row_event_type, schema, table, primary_key)
        self.before_row_vals = before_row_vals
        self.after_row_vals = after_row_vals
    
    def to_dict(self):
        body = super().to_dict()
        body['before_row_vals'] = self.before_row_vals
        body['after_row_vals'] = self.after_row_vals
        return body
    
    def get_routing_key(self):
        if self.primary_key != None:
            if isinstance(self.primary_key, tuple):
                values = []
                for key in self.primary_key:                    
                    values.append(self.after_row_vals[key])
                return ",".join(values)

            elif isinstance(self.primary_key, str):
                return str(self.after_row_vals[self.primary_key])
        
        return None