#!/bin/bash
KAFKA_CONTAINER=$(docker ps --filter "ancestor=confluentinc/cp-kafka" -q)
if [ -z "$KAFKA_CONTAINER" ]; then
  echo "Start containers first"
  exit 1
fi

docker exec -it $KAFKA_CONTAINER \
  kafka-topics --create --topic orders --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

docker exec -it $KAFKA_CONTAINER \
  kafka-topics --create --topic orders-dlq --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

docker exec -it $KAFKA_CONTAINER \
  kafka-topics --create --topic orders-agg --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

echo "Topics created"
