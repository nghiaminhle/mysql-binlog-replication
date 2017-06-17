class ReadLogError(Exception):
    def __init__(self, value):
        self.message = value
    