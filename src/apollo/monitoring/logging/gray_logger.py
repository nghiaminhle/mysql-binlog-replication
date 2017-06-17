from Apollo.Monitoring.Logging.Logger import Logger

class GrayLogger(Logger):
    
    def write_entry(self, entry):
        Logger.write_entry(entry)