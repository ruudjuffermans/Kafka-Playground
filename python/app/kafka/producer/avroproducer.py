#!/usr/bin/env python

import sys
import os
import time
from random import choice
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from uuid import uuid4

from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from kafka.registry import SchemaRegistry
from kafka.producer import BaseProducer




class Car(object):
    """
    Car record

    Args:
        name (str): Car's name

        speed (int): Car's speed

        color (str): Car's color
    """

    def __init__(self, name, speed, color):
        self.name = name
        self.speed = speed
        self.color = color


def car_to_dict(car, ctx):
    """
    Returns a dict representation of a car instance for serialization.

    Args:
        car (car): car instance.

        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.

    Returns:
        dict: Dict populated with car attributes to be serialized.
    """

    # User._address must not be serialized; omit from dict
    return dict(name=car.name,
                speed=car.speed,
                color=car.color)


class AvroProducer(BaseProducer):
    def __init__(self, config):
        super().__init__(config)
        self.sr = SchemaRegistry()
        self.string_serializer = StringSerializer('utf_8')

        schema_str = self.sr.schema_register("topic_avro", "AVRO")
        self.avro_serializer = AvroSerializer(self.sr,
                                     schema_str,
                                     car_to_dict)
        self.names = ['eabara', 'jsmith', 'sgarcia', 'jbernard', 'htanaka', 'awalther']
        self.colors = ['book', 'alarm clock', 't-shirts', 'gift card', 'batteries']


    def callback(self, err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
                topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))

    def run(self, num_messages=100, timeout=.1):
        for _ in range(num_messages):
            self.poll(0.0)
            try:
                car = Car(name=choice(self.names),
                            color=choice(self.colors),
                            speed=1)
                self.produce(topic="topic_avro",
                             key=self.string_serializer(str(uuid4())),
                             value=self.avro_serializer(car, SerializationContext("topic_avro", MessageField.VALUE)),
                             on_delivery=self.callback)

                time.sleep(timeout)
            except KeyboardInterrupt:
                break
            except ValueError:
                print("Invalid input, discarding record...")
                continue

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])

    producer = AvroProducer(config)
    try:
        producer.produce()
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        print("Producer execution completed")
