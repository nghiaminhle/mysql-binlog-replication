from apollo.log.log_metadata import LogMetadata

class BinLogMetadata(LogMetadata):
    _log_pos = 0
    _log_file = None

    def __init__(self, log_pos=0, log_file=None, schema=None, table=None, row_id=None):
        super().__init__(schema, table, row_id)
        self._log_pos = log_pos
        self._log_file = log_file
    
    @property
    def log_pos(self):
        return self._log_pos
    
    @log_pos.setter
    def log_pos(self, value):
        self._log_pos = value

    @property    
    def log_file(self):
        return self._log_file
    
    @log_file.setter
    def log_file(self, value):
        self._log_file = value
    
    @property
    def key(self):
        return "{},{}".format(self._log_file, self._log_pos)