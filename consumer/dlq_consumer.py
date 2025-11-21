from confluent_kafka import Consumer
from utils import avro_deserializer

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "dlq-group",
    "auto.offset.reset": "earliest"
})

consumer.subscribe(["orders_dlq"])

print("Listening to DLQ...")

while True:
    msg = consumer.poll(1.0)
    if msg:
        print("DLQ MESSAGE:", avro_deserializer(msg.value()))
