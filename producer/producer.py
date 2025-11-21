import random
import json
from confluent_kafka import SerializingProducer
from fastavro import parse_schema, schemaless_writer
from io import BytesIO

# Load Avro schema
import json
schema = json.load(open("../avro/order.avsc"))
parsed_schema = parse_schema(schema)

def avro_serializer(obj, ctx):
    bytes_writer = BytesIO()
    schemaless_writer(bytes_writer, parsed_schema, obj)
    return bytes_writer.getvalue()

producer = SerializingProducer({
    "bootstrap.servers": "localhost:9092",
    "value.serializer": avro_serializer
})

products = ["Item1", "Item2", "Item3"]

for i in range(1, 21):
    order = {
        "orderId": str(i),
        "product": random.choice(products),
        "price": round(random.uniform(10, 100), 2)
    }

    producer.produce("orders", value=order)
    print("Produced:", order)

producer.flush()
