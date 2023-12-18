#!/bin/bash
docker exec -i kafka-schema-registry kafka-avro-console-consumer --bootstrap-server kafka101:19092 --topic wikimedia --from-beginning --property schema.registry.url=http://kafka-schema-registry:8081
