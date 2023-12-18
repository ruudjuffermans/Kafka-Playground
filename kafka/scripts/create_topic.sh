#!/bin/bash
docker exec -i kafka101 kafka-topics --create --topic car1 --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092
