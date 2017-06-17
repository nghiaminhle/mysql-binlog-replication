
class MessageBuilderError(Exception):    
    
    def __init__(self, not_existed_key, bin_log_vals):
        self.not_existed_key = not_existed_key
        self.bin_log_vals = bin_log_vals
        self.message = "Binlog format is invalid. " + not_existed_key + " not exited."