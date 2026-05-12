from kafka import ConsumerRebalanceListener, OffsetAndMetadata, TopicPartition


class ConsumerRebalanceListenerHandler(ConsumerRebalanceListener):
    def __init__(self, consumer):
        self.consumer = consumer
        self.current_offset = {}

    def get_current_offset(self):
        return self.current_offset

    def add_offset(self, topic, partition, offset):
        key = TopicPartition(topic, partition)
        self.current_offset[key] = OffsetAndMetadata(offset + 1, 'commit', -1)

    def on_partitions_assigned(self, assigned):
        pass

    def on_partitions_revoked(self, revoked):
        if self.current_offset:
            self.consumer.commit(self.current_offset)
        self.current_offset = {}
