from apollo.messaging.message import Message
from datetime import datetime
from .message_builder_error import MessageBuilderError

class MessageBuilder:

    def build(self, attributes)->Message:
        msg = Message()
        msg.message_id = self._try_get(attributes, 'message_id')
        msg.payload = self._try_get(attributes, 'payload')
        msg.created_at = self._try_get(attributes, 'created_at')
        msg.event_type = self._try_get(attributes, 'type')
        msg.object_id = self._try_get(attributes, 'object_id')
        msg.source = self._try_get(attributes, 'source')
        msg.destination =self._try_get(attributes, 'destination')
        return msg

    def _try_get(self, attributes, key):
        try:
            return attributes[key]
        except:
            raise MessageBuilderError(key, attributes)