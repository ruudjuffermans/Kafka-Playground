
from argparse import ArgumentParser, FileType
from configparser import ConfigParser

from kafka.producer import AvroProducer, JsonProducer, BaseProducer
from kafka.consumer import BaseConsumer, AvroConsumer, JsonConsumer


parser = ArgumentParser()
parser.add_argument('config_file', type=FileType('r'))
args = parser.parse_args()

config_parser = ConfigParser()
config_parser.read_file(args.config_file)
config = dict(config_parser['default'])

# try:
#     producer = JsonProducer(config)
#     producer.run()
# except Exception as e:
#     print(f"An error occurred: {e}")
# finally:
#     print("Producer execution completed")



# config_parser = ConfigParser()
# config_parser.read_file(args.config_file)
# config = dict(config_parser['default'])
config.update(config_parser['consumer'])

consumer = JsonConsumer(config, ["topic_avro"], reset=True)

try:
    consumer.consume_loop()
except KeyboardInterrupt:
    consumer.shutdown()
finally:
    print("Consumer execution completed")