#!/bin/sh

docker-compose up -d

echo "Waiting for Kafka to launch on 9092..."
while ! nc -z localhost 9092; do   
  sleep 1.0
  echo "Kafka not yet ready..."
done 
echo "Kafka is now ready!"

echo "Building out topics..."
kafka-topics --bootstrap-server=kafka1:9092 --create --topic consent.Client
kafka-topics --bootstrap-server=kafka1:9092 --create --topic changeOfAddress
kafka-topics --bootstrap-server=kafka1:9092 --create --topic genericTopic
