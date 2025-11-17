import time, json, uuid
from confluent_kafka import Producer
from fastavro import parse_schema, schemaless_writer
from io import BytesIO

# Load schema
with open("order.avsc", "r") as f:
    import json as _json
    schema = parse_schema(_json.load(f))

KAFKA_BROKER = "localhost:9092"
TOPIC = "orders"

p = Producer({"bootstrap.servers": KAFKA_BROKER})

def avro_serialize(schema, record):
    b = BytesIO()
    schemaless_writer(b, schema, record)
    return b.getvalue()

def produce_order(order):
    # Use orderId as key so messages with same orderId stick to same partition (helps ordering)
    key = order["orderId"].encode("utf-8")
    value = avro_serialize(schema, order)
    p.produce(TOPIC, key=key, value=value)
    p.flush()

if __name__ == "__main__":
    # produce sample orders in a loop
    products = ["Item1", "Item2", "Item3"]
    i = 0
    while i < 50:
        order = {
            "orderId": str(uuid.uuid4()),
            "product": products[i % len(products)],
            "price": float(10 + (i % 5) * 2.5),
            "timestamp": int(time.time() * 1000)
        }
        produce_order(order)
        print("Produced:", order)
        i += 1
        time.sleep(0.2)
