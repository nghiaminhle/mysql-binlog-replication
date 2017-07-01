import unittest
from apollo.events.event_dispatcher import EventDispatcher
from apollo.events.event_handler import EventHandler
from apollo.events.event import Event

class TestEventDispatcher(unittest.TestCase):

    def test_has_not_existed_event_handler(self):
        dispatcher = EventDispatcher()
        handler = EventHandler()
        has = dispatcher.has_event_handler('test', handler)
        self.assertFalse(has)
    
    def test_add_event_handler(self):
        dispatcher = EventDispatcher()
        handler = EventHandler()
        dispatcher.add_event_handler('test', handler)
        has = dispatcher.has_event_handler('test', handler)
        self.assertTrue(has)
    
    def test_remove_event_handler(self):
        dispatcher = EventDispatcher()
        handler = EventHandler()
        dispatcher.add_event_handler('test', handler)
        dispatcher.remove_event_handler('test', handler)
        has = dispatcher.has_event_handler('test', handler)
        self.assertFalse(has)
    
    def test_dispatch(self):
        event = DummyEvent(1)
        dispatcher = EventDispatcher()

        increase_handler = DummyIncreaseHandler(10)
        dispatcher.add_event_handler('DummyEvent', increase_handler)

        decrease_handler = DummyDecreaseHandler(10)
        dispatcher.add_event_handler('DummyEvent', decrease_handler)

        dispatcher.dispatch(event)

        self.assertEqual(11, increase_handler.value)
        self.assertEqual(9, decrease_handler.value)        

class DummyEvent(Event):
    value = 0
    def __init__(self, value):
        super().__init__('DummyEvent')
        self.value = value

class DummyIncreaseHandler(EventHandler):
    value = 0
    def __init__(self, value):
        self.value = value

    def handle(self, event):
        self.value = self.value + event.value

class DummyDecreaseHandler(EventHandler):
    value = 0
    def __init__(self, value):
        self.value = value
    
    def handle(self, event):
        self.value = self.value - event.value