from Apollo.Messaging.KafkaEnvelope import KafkaEnvelope
from Apollo.Messaging.MessageBuilder import MessageBuilder
from .BinLog import BinLog
from .BinLogMetadata import BinLogMetadata

class KafkaEnvelopeBuilder:

    # Convert BinLog to Kafka Envelope
    def build(self, bin_log) -> KafkaEnvelope:
        messageBuilder = MessageBuilder()
        message = messageBuilder.build(bin_log.row_vals)
        log_metadata = BinLogMetadata(
                log_pos=bin_log.log_pos,
                log_file=bin_log.log_file,
                schema=bin_log.schema,
                table=bin_log.table,
                row_id=bin_log.row_id
            )
        eventlope = KafkaEnvelope(
            message, 
            log_metadata, 
            bin_log.row_vals['topic'], 
            bin_log.row_vals['routing_key'])
        return eventlope
