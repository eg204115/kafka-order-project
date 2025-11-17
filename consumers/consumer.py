import time
import json
from confluent_kafka import Consumer, Producer, KafkaError
from fastavro import parse_schema, schemaless_reader
from io import BytesIO

# Load schema
with open("order.avsc", "r") as f:
    import json as _json
    schema = parse_schema(_json.load(f))

KAFKA_BROKER = "localhost:9092"
INPUT_TOPIC = "orders"
DLQ_TOPIC = "orders-dlq"
AGG_TOPIC = "orders-agg"   # optional topic to publish aggregation snapshots

consumer_conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'order-consumer-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
}

prod_conf = {"bootstrap.servers": KAFKA_BROKER}

consumer = Consumer(consumer_conf)
producer = Producer(prod_conf)

consumer.subscribe([INPUT_TOPIC])

# state for running average per product
state = {}  # product -> {'count': int, 'avg': float}

def avro_deserialize(schema, value_bytes):
    b = BytesIO(value_bytes)
    return schemaless_reader(b, schema)

def send_to_dlq(key, value_bytes, error_reason):
    headers = [("error", error_reason.encode("utf-8"))]
    producer.produce(DLQ_TOPIC, key=key, value=value_bytes, headers=headers)
    producer.flush()

def publish_agg_snapshot(product):
    rec = {
        "product": product,
        "count": state[product]['count'],
        "avg_price": state[product]['avg'],
        "timestamp": int(time.time() * 1000)
    }
    # for demo: publish JSON into AGG_TOPIC
    producer.produce(AGG_TOPIC, key=product.encode("utf-8"), value=json.dumps(rec).encode("utf-8"))
    producer.flush()

def process_order(order):
    # Example processing: update running average per product
    product = order['product']
    price = float(order['price'])
    if product not in state:
        state[product] = {'count': 0, 'avg': 0.0}
    s = state[product]
    s['count'] += 1
    # running average formula
    s['avg'] += (price - s['avg']) / s['count']
    # publish periodic snapshots (every 10 messages)
    if s['count'] % 10 == 0:
        publish_agg_snapshot(product)
    # Simulate occasional transient error for demo
    if price < 0:
        raise ValueError("Invalid price")

RETRY_MAX = 3
RETRY_BACKOFF = 1.0  # seconds, will multiply

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            # handle error (log, continue)
            print("Consumer error:", msg.error())
            continue

        key = msg.key()  # bytes or None
        value_bytes = msg.value()

        try:
            order = avro_deserialize(schema, value_bytes)
        except Exception as e:
            print("Deserialization error:", e)
            # send to DLQ with reason
            send_to_dlq(key, value_bytes, "deserialization_error")
            consumer.commit(msg)  # commit to avoid reprocessing
            continue

        # processing with retry logic
        tries = 0
        while True:
            try:
                process_order(order)
                # success -> commit offset
                consumer.commit(msg)
                break
            except Exception as e:
                tries += 1
                print(f"Processing error (try {tries}) for order {order.get('orderId')}: {e}")
                if tries >= RETRY_MAX:
                    print("Max retries reached, sending to DLQ")
                    send_to_dlq(key, value_bytes, f"processing_error:{str(e)}")
                    consumer.commit(msg)
                    break
                else:
                    backoff = RETRY_BACKOFF * (2 ** (tries - 1))
                    print(f"Retrying after {backoff}s")
                    time.sleep(backoff)

except KeyboardInterrupt:
    print("Shutting down consumer")
finally:
    consumer.close()
