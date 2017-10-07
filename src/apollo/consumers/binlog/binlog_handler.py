from apollo.consumers.message_handler import MessageHandler

class BinLogHandler(MessageHandler):

    def handle(self, message):
        values = message.value.decode()
        print(values)