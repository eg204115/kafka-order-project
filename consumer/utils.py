from fastavro import parse_schema, schemaless_reader
from io import BytesIO
import json

schema = json.load(open("../avro/order.avsc"))
parsed_schema = parse_schema(schema)

def avro_deserializer(data):
    bytes_reader = BytesIO(data)
    return schemaless_reader(bytes_reader, parsed_schema)
