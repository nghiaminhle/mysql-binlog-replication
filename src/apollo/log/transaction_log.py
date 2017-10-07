class TransactionLog:
    _schema = None # schema database
    _table = None #name of table
    _row_id = 0 # unique id of row
    _row_vals = None # row value in key value

    def __init__(self, schema, table, row_vals):
        self._schema = schema
        self._table = table
        self._row_vals = row_vals
        self._row_id = row_vals['id']

    @property
    def schema(self):
        return self._schema

    @schema.setter
    def schema(self, value):
        self._schema = value

    @property
    def table(self):
        return self._table
    
    @table.setter
    def table(self, value):
        self._table = value
    
    @property
    def row_vals(self):
        return self._row_vals
    
    @row_vals.setter
    def row_vals(self, value):
        self._row_vals = value
    
    @property
    def row_id(self):
        return self._row_id
    
    @row_id.setter
    def row_id(self, value):
        self._row_id = value