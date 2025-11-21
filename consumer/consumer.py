import time
from confluent_kafka import Consumer, Producer
from utils import avro_deserializer

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "order-consumers",
    "auto.offset.reset": "earliest"
})

dlq_producer = Producer({"bootstrap.servers": "localhost:9092"})
consumer.subscribe(["orders"])

total = 0
count = 0

def process_message(msg):
    global total, count
    data = avro_deserializer(msg.value())
    
    # Simulate random failure
    if float(data["price"]) > 90:
        raise Exception("High price trigger failure")
    
    total += data["price"]
    count += 1
    print("Processing:", data, " | Running Avg:", round(total / count, 2))

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue

    retries = 3
    success = False

    for attempt in range(1, retries+1):
        try:
            process_message(msg)
            success = True
            break
        except Exception as e:
            print(f"Retry {attempt}/3 failed:", e)
            time.sleep(1)

    if not success:
        print("Sending to DLQ:", msg.value())
        dlq_producer.produce("orders_dlq", value=msg.value())
        dlq_producer.flush()
