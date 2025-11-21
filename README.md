# kafka-order-project

This project implements a Kafka-based distributed messaging system for processing **order messages**.  
It uses:

- **Kafka + Zookeeper** (Docker)
- **Python Producer** (Avro serialization)
- **Python Consumer** (Real-time aggregation)
- **Retry Logic** for temporary failures
- **Dead Letter Queue (DLQ)** for permanently failed messages

## 🚀 Features

### ✔ Avro Serialization  
Every order is serialized using an Avro schema (`order.avsc`).

### ✔ Real-Time Aggregation  
The consumer calculates a **running average of product prices**.

### ✔ Retry Logic  
If processing fails, the consumer retries the message **3 times**.

### ✔ DLQ (Dead Letter Queue)  
Messages that still fail after retries are sent to the **orders_dlq** topic.

### ✔ Dockerized Kafka Setup  
Kafka and Zookeeper run inside Docker containers using `docker-compose`.

✅ RUNNING COMMANDS
# 1️⃣ Navigate into the project folder
cd kafka-order-project

# 2️⃣ Start Kafka + Zookeeper using Docker
docker-compose up -d

# 3️⃣ Check if containers are running
docker ps

# 4️⃣ Enter Kafka container
docker exec -it kafka-order-project-kafka-1 bash

# 5️⃣ Create the required Kafka topics
kafka-topics --create --topic orders --bootstrap-server localhost:9092
kafka-topics --create --topic orders_dlq --bootstrap-server localhost:9092

# 6️⃣ Verify that the topics were created
kafka-topics --list --bootstrap-server localhost:9092

# 7️⃣ Exit Kafka container
exit

# 8️⃣ Install Python dependencies
pip install -r requirements.txt

# 9️⃣ Run the consumer (keeps running)
python consumer/consumer.py

# 🔟 Open a new terminal — run the producer to send messages
python producer/producer.py

# 1️⃣1️⃣ (Optional) Run DLQ consumer to view failed messages
python consumer/dlq_consumer.py

# 1️⃣2️⃣ Stop all Docker containers
docker-compose down

