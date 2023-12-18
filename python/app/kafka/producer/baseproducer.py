#!/usr/bin/env python

import sys
import time
from random import choice
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer

class BaseProducer(Producer):
    def __init__(self, config):
        super().__init__(config)
        self.names = ['eabara', 'jsmith', 'sgarcia', 'jbernard', 'htanaka', 'awalther']

    def callback(self, err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            print("Consumed event from topic {topic}: value = {value:12}".format(
                topic=msg.topic(), value=msg.value().decode('utf-8')))

    def run(self, num_messages=100):
        for _ in range(num_messages):
            name = choice(self.names)
            self.produce("topic", name, callback=self.callback)

            self.poll(10000)
            self.flush()
