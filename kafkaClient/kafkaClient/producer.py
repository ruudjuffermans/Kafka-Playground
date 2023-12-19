from confluent_kafka import Producer
from utils.config import get_config


class KafkaProducer(Producer):
    def __init__(self, config={}):
        self.config = get_config("producer", config)
        super().__init__(self.config)

    def msg_process(self, msg):
        raise NotImplementedError("This method should be overridden in a subclass")

    def callback(self, err, msg):
        if err:
            print("ERROR: Message failed delivery: {}".format(err))
        else:
            print(
                "Consumed event from topic {topic}: value = {value:12}".format(
                    topic=msg.topic(), value=msg.value().decode("utf-8")
                )
            )

    def produce(self, topic, value):
        super().produce(topic, value, callback=self.callback)


if __name__ == "__main__":
    producer = KafkaProducer()
    try:
        producer.produce("test_", "vallll")
        producer.poll(10000)
        producer.flush()
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        print("Producer execution completed")
