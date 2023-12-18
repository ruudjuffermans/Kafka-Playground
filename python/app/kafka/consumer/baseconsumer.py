#!/usr/bin/env python

import sys
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Consumer, KafkaException, KafkaError, OFFSET_BEGINNING
MIN_COMMIT_COUNT = 3

def sync_commit(instance):
    def wrapper(msg):
        instance._msg_process(msg)
        instance.msg_count += 1
        if instance.msg_count % MIN_COMMIT_COUNT == 0:
            instance.commit(asynchronous=False)
    return wrapper

def async_commit(instance):
    def wrapper(msg):
        instance._msg_process(msg)
        instance.msg_count += 1
        if instance.msg_count % MIN_COMMIT_COUNT == 0:
            instance.commit(asynchronous=True)
    return wrapper


def guaranteed_commit(instance):
    def wrapper(msg):
        instance.commit(asynchronous=False)
        instance._msg_process(msg)
    return wrapper



class BaseConsumer(Consumer):
    def __init__(self, config, topics, commit_mode='sync', reset=False):
        super().__init__(config)
        self.topics = topics
        self.running = True
        self.should_reset = reset
        self.reset = reset
        self.msg_count = 0
        self.set_commit_mode(commit_mode)

    def reset_offset(self, partitions):
        for p in partitions:
            p.offset = OFFSET_BEGINNING
        self.assign(partitions)

    def shutdown(self):
        self.running = False

    def set_commit_mode(self, mode):
        if mode == 'sync':
            self.msg_process = sync_commit(self)
        elif mode == 'async':
            self.msg_process = async_commit(self)
        elif mode == 'guaranteed':
            self.msg_process = guaranteed_commit(self)
        else:
            raise ValueError("Invalid processing mode")

    def msg_process(self, msg):
        raise NotImplementedError("This method should be overridden in a subclass")
    
    def _msg_process(self, msg):
        print("Consumed event from topic {topic}: key = {key:12} value = {value:12}".format(
            topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))


    def on_assign(self, consumer, partitions):
        if self.should_reset:
             self.reset_offset(partitions) 

    def consume_loop(self):
        try:
  
            self.subscribe(self.topics, on_assign=self.on_assign)

            while self.running:
                msg = self.poll(1.0)
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

