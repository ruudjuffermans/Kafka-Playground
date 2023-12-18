from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
import os

class SchemaRegistry(SchemaRegistryClient):
    def __init__(self, schema_registry_url="http://kafka-schema-registry:8081"):
        super().__init__({'url': schema_registry_url})

    def schema_get(self, subject):
        print(subject)
        latest_version = self.get_latest_version(subject_name=subject)
        print(latest_version)
        
        return latest_version.schema.schema_str

    def schema_register(self, subject, schema_type):
        # Validate schema_type
        if schema_type not in ['JSON', 'AVRO', 'PROTOBUF']:
            raise ValueError("Invalid schema type. Must be 'JSON', 'AVRO', or 'PROTOBUF'.")

        # Determine file extension based on schema_type
        extension = {
            'JSON': 'json',
            'AVRO': 'avsc',
            'PROTOBUF': 'proto'
        }.get(schema_type, 'json')

        print(subject)
        path = os.path.realpath(os.path.dirname(__file__))
        
        with open(f"{path}/../schemas/{subject}.{extension}") as f:
            schema_str = f.read()

        schema = Schema(schema_str, schema_type=schema_type)
        self.register_schema(subject_name=subject, schema=schema)
        return schema_str

    def update_schema(self, subject, schema_str):
        versions_deleted_list = self.delete_subject(subject)
        print(f"versions of schema deleted list: {versions_deleted_list}")

        schema_id = self.register_schema(subject, schema_str)
        return schema_id
