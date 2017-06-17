class Message:
    _message_id = None
    _created_at = None
    _event_type = None
    _object_id = None
    _source = None
    _destination = None
    _payload = None

    def to_dict(self):
        return {
            'message_id': self.message_id,
            #'created_at': self.created_at,
            'event_type': self.event_type,
            'object_id': self.object_id,
            'source': self.source,
            'destination': self.destination,
            'payload': self.payload
        }

    @property
    def message_id(self):
        return self._message_id
    
    @message_id.setter
    def message_id(self, value):
        self._message_id = value
    
    @property
    def created_at(self):
        return self._created_at
    
    @created_at.setter
    def created_at(self, value):
        self._created_at = value
    
    @property
    def event_type(self):
        return self._event_type

    @event_type.setter
    def event_type(self, value):
        self._event_type = value
    
    @property
    def object_id(self):
        return self._object_id
    
    @object_id.setter
    def object_id(self, value):
        self._object_id = value

    @property
    def source(self):
        return self._source
    
    @source.setter
    def source(self, value):
        self._source = value
    
    @property
    def destination(self):
        return self._destination
    
    @destination.setter
    def destination(self, value):
        self._destination = value

    @property
    def payload(self):
        return self._payload
    
    @payload.setter
    def payload(self, value):
        self._payload = value