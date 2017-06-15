from Apollo.Monitoring.Logging.Logger import Logger

class FileLogger(Logger):
    _log_file = None

    def __init__(self, log_file):
        self._log_file = log_file
    
    def write_entry(self, entry):
        Logger.write_entry(entry)
    
    @property
    def log_file(self):
        return self._log_file