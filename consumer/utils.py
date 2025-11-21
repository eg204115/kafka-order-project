from fastavro import parse_schema, schemaless_reader
from io import BytesIO
import json
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SCHEMA_PATH = os.path.join(BASE_DIR, "..", "avro", "order.avsc")

# Load schema
with open(SCHEMA_PATH, "r") as f:
    schema = json.load(f)

parsed_schema = parse_schema(schema)

def avro_deserializer(data):
    bytes_reader = BytesIO(data)
    return schemaless_reader(bytes_reader, parsed_schema)
