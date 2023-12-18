import os
import sys
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.serialization import StringDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka import KafkaException, KafkaError, OFFSET_BEGINNING
from kafka.registry import SchemaRegistry
from kafka.consumer import BaseConsumer
MIN_COMMIT_COUNT = 3
class User(object):
    """
    User record

    Args:
        name (str): User's name

        favorite_number (int): User's favorite number

        speed (str): User's favorite color
    """

    def __init__(self, name=None, color=None, speed=None):
        self.name = name
        self.color = color
        self.speed = speed


def dict_to_user(obj, ctx):
    """
    Converts object literal(dict) to a User instance.

    Args:
        obj (dict): Object literal(dict)

        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
    """

    if obj is None:
        return None

    return User(name=obj['name'],
                color=obj['color'],
                speed=obj['speed'])


class JsonConsumer(BaseConsumer):
    def __init__(self, config, topics, commit_mode='sync', reset=False):
        super().__init__(config, topics, commit_mode=commit_mode,reset=reset)
        self.sr = SchemaRegistry()
        self.json_deserializer = JSONDeserializer(self.sr,
                                         self.sr.schema_get("topic_json"),
                                         dict_to_user)
        self.topics = topics


    def _msg_process(self, msg):
        user = self.json_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
        if user is not None:
            print("User record {}: name: {}\n"
                "\tcolor: {}\n"
                "\tspeed: {}\n"
                .format(msg.key().decode('utf-8'), user.name,
                        user.speed,
                        user.color))

    def consume_loop(self):
        try:
  
            self.subscribe(self.topics, on_assign=self.on_assign)

            while self.running:
                msg = self.poll(timeout=1.0)
                if msg is None:
                    continue
                else:
                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                            (msg.topic(), msg.partition(), msg.offset()))
                        else:
                            raise KafkaException(msg.error())
                    else:

                        self.msg_process(msg)

        finally:
            self.close()


