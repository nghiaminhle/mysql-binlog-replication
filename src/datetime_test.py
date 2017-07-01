import json
from datetime import datetime
class Person:
    name = ""
    created_at = None

    def __init__(self):
        self.name = 'test'
        self.created_at = datetime.now()
    
    def to_dict(self):
        return {'name':self.name, 'created_at':self.created_at}

def datetime_handler(x):
    if isinstance(x, datetime):
        return x.__str__()
    

#json.dumps(data, default=datetime_handler)

person = Person()
print(person.name)
payload = json.dumps(person.to_dict(), sort_keys=True, default=datetime_handler)
print(payload)