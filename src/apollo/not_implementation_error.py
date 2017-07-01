class NotImplementationError(Exception):
    
    def __init__(self):
        self.message = 'Function is not implemented'

    def __str__(self):
        return self.message