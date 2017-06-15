from Apollo.Log.TransactionLog import TransactionLog

class BinLog(TransactionLog):
    _log_file = None
    _log_pos = 0

    def __init__(self, schema, table, row_vals, log_file, log_pos):
        TransactionLog.__init__(self, schema, table, row_vals)
        self._log_file = log_file
        self._log_pos = log_pos
    
    @property
    def log_file(self):
        return self._log_file

    @log_file.setter
    def log_file(self, value):
        self._log_file = value
    
    @property
    def log_pos(self):
        return self._log_pos
        
    @log_pos.setter
    def log_pos(self, value):
        self._log_pos = value