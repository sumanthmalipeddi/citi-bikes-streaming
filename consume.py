import os
import time

from kafka_consumer import Consumer
from services import S3Service
from constants import BIKES_STATIONS_INFORMATION_TOPIC, BIKES_STATIONS_STATUS_TOPIC

S3_BUCKET = os.environ["S3_BUCKET"]
AWS_REGION = os.environ.get("AWS_REGION", "ap-south-1")

BATCH_SIZE = 500
FLUSH_INTERVAL_SEC = 60

TOPIC_PREFIX = {
    BIKES_STATIONS_INFORMATION_TOPIC: "station_information",
    BIKES_STATIONS_STATUS_TOPIC: "station_status",
}


class Consume:
    def __init__(self):
        self.consumer_group = Consumer("citi_bike_stations")
        self.s3 = S3Service(bucket=S3_BUCKET, region=AWS_REGION)
        self.buffers = {topic: [] for topic in TOPIC_PREFIX}
        self.last_flush = {topic: time.time() for topic in TOPIC_PREFIX}

    def flush(self, topic):
        records = self.buffers[topic]
        if not records:
            return

        values = [r.value for r in records]
        key = self.s3.upload_jsonl(TOPIC_PREFIX[topic], values)
        print(f"flushed {len(records)} -> s3://{S3_BUCKET}/{key}")

        for r in records:
            self.consumer_group.rebalance_listener.add_offset(
                r.topic, r.partition, r.offset
            )
        self.consumer_group.consumer.commit(
            self.consumer_group.rebalance_listener.get_current_offset()
        )

        self.buffers[topic] = []
        self.last_flush[topic] = time.time()

    def should_flush(self, topic):
        if len(self.buffers[topic]) >= BATCH_SIZE:
            return True
        if (
            self.buffers[topic]
            and time.time() - self.last_flush[topic] >= FLUSH_INTERVAL_SEC
        ):
            return True
        return False

    def consumer(self):
        self.consumer_group.subscribe_consumer(list(TOPIC_PREFIX.keys()))
        try:
            while True:
                polled = self.consumer_group.consumer.poll(timeout_ms=1000)
                for tp, msgs in polled.items():
                    if tp.topic in self.buffers:
                        self.buffers[tp.topic].extend(msgs)

                for topic in list(self.buffers.keys()):
                    if self.should_flush(topic):
                        self.flush(topic)
        except KeyboardInterrupt:
            print("shutting down, flushing remaining buffers...")
            for topic in list(self.buffers.keys()):
                self.flush(topic)


if __name__ == "__main__":
    Consume().consumer()
