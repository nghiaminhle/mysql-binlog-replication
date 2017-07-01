class BinLogCondition:

    def is_satisfy(self, binlogevent, row):
        return False
    
    def build_envelope(self, binlogevent, row, log_pos, log_file):
        return None
    