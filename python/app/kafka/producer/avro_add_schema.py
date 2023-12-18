# 3rd party library imported
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry import Schema

# imort from constants
SCHEMA_STR="{\"type\":\"record\",\"name\":\"value_wikimedia\",\"namespace\":\"wikimedia\",\"fields\":[{\"name\":\"bot\",\"type\":[\"null\",\"boolean\"],\"default\":null},{\"name\":\"comment\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"id\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"length\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"Length\",\"fields\":[{\"name\":\"new\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"old\",\"type\":[\"null\",\"int\"],\"default\":null}]}],\"default\":null},{\"name\":\"meta\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"Meta\",\"fields\":[{\"name\":\"domain\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"dt\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"id\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"offset\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"partition\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"request_id\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"stream\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"topic\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"uri\",\"type\":[\"null\",\"string\"],\"default\":null}]}],\"default\":null},{\"name\":\"minor\",\"type\":[\"null\",\"boolean\"],\"default\":null},{\"name\":\"namespace\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"parsedcomment\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"patrolled\",\"type\":[\"null\",\"boolean\"],\"default\":null},{\"name\":\"revision\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"Revision\",\"fields\":[{\"name\":\"new\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"old\",\"type\":[\"null\",\"int\"],\"default\":null}]}],\"default\":null},{\"name\":\"schema\",\"type\":[\"null\",\"string\"],\"doc\":\"Theoriginalfieldnamewas'$schema'butsomecharactersisnotacceptedinthefieldnameofAvrorecord\",\"default\":null},{\"name\":\"server_name\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"server_script_path\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"server_url\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"timestamp\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"title\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"type\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"user\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"wiki\",\"type\":[\"null\",\"string\"],\"default\":null}]}"


schema_registry_url = 'http://kafka-schema-registry:8081'
kafka_topic = 'wikimedia'
schema_registry_subject = f"{kafka_topic}"

def get_schema_from_schema_registry(schema_registry_url, schema_registry_subject):
    sr = SchemaRegistryClient({'url': schema_registry_url})
    latest_version = sr.get_latest_version(schema_registry_subject)

    return sr, latest_version

def register_schema(schema_registry_url, schema_registry_subject, schema_str):
    sr = SchemaRegistryClient({'url': schema_registry_url})
    schema = Schema(schema_str, schema_type="AVRO")
    schema_id = sr.register_schema(subject_name=schema_registry_subject, schema=schema)

    return schema_id

def update_schema(schema_registry_url, schema_registry_subject, schema_str):
    sr = SchemaRegistryClient({'url': schema_registry_url})
    versions_deleted_list = sr.delete_subject(schema_registry_subject)
    print(f"versions of schema deleted list: {versions_deleted_list}")

    schema_id = register_schema(schema_registry_url, schema_registry_subject, schema_str)
    return schema_id

schema_id = register_schema(schema_registry_url, schema_registry_subject, SCHEMA_STR)
print(schema_id)

sr, latest_version = get_schema_from_schema_registry(schema_registry_url, schema_registry_subject)
print(latest_version.schema.schema_str)