#!/bin/sh

docker compose up -d

echo "Waiting for Kafka to launch on 9092..."
while ! nc -z localhost 9092; do   
  sleep 1.0
  echo "Kafka not yet ready..."
done 
echo "Kafka is now ready!"

echo "Building out topics..."
kafka-topics.sh --bootstrap-server=kafka1:9092 --create --topic changeOfAddress --partitions 2
kafka-topics.sh --bootstrap-server=kafka1:9092 --create --topic consent.Client --partitions 2
kafka-topics.sh --bootstrap-server=kafka1:9092 --create --topic genericTopic --partitions 2
