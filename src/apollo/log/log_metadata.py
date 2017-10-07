class LogMetadata:
    _schema = None
    _table = None
    _row_id = None

    def __init__(self, schema, table, row_id):
        self._schema = schema
        self._table = table
        self._row_id = row_id

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
    def row_id(self):
        return self._row_id
    
    @row_id.setter
    def row_id(self, value):
        self._row_id = value