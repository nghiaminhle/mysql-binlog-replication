from Apollo.Monitoring.Logging.Logger import Logger

class LogManager(Logger):

    def write_entry(self, entry):
        Logger.write_entry(entry)