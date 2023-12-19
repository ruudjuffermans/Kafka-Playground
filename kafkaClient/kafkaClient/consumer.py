import sys
from confluent_kafka import Consumer, KafkaException, KafkaError
from utils.config import get_config

data = ["eabara", "jsmith", "sgarcia", "jbernard", "htanaka", "awalther"]


class KafkaConsumer(Consumer):
    def __init__(self, topics, reset=False, config={}):
        self.config = get_config("consumer", config)
        super().__init__(self.config)
        self.topics = topics
        self.running = True
        self.reset = reset

    def msg_process(self, msg):
        print(
            "Consumed event from topic {topic}: value = {value:12}".format(
                topic=msg.topic(),
                value=msg.value().decode("utf-8"),
            )
        )

    def shutdown(self):
        self.running = False

    def on_assign(self, consumer, partitions):
        if self.reset:
            self.reset_offset(partitions)

    def subscribe(self, topics):
        super().subscribe(topics, on_assign=self.on_assign)


if __name__ == "__main__":
    consumer = KafkaConsumer(["topic3"])
    try:
        consumer.subscribe(["test_2"])
        msg = consumer.poll(20.0)
        if msg is None:
            print("none")
        else:
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    sys.stderr.write(
                        "%% %s [%d] reached end at offset %d\n"
                        % (msg.topic(), msg.partition(), msg.offset())
                    )
                else:
                    raise KafkaException(msg.error())
            else:
                print(msg.value())

    finally:
        consumer.close()
