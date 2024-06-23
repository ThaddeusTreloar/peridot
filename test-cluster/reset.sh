#!/bin/bash

kafka-topics.sh --bootstrap-server=kafka1:9092 --topic changeOfAddress --delete
kafka-topics.sh --bootstrap-server=kafka1:9092 --topic changeOfAddress --create --partitions 2
kafka-topics.sh --bootstrap-server=kafka1:9092 --topic consent.Client --delete
kafka-topics.sh --bootstrap-server=kafka1:9092 --topic consent.Client --create --partitions 2
kafka-topics.sh --bootstrap-server=kafka1:9092 --topic app-message-closures-consent_table-Changelog --delete
kafka-topics.sh --bootstrap-server=kafka1:9092 --topic genericTopic --delete
kafka-topics.sh --bootstrap-server=kafka1:9092 --topic genericTopic --create --partitions 2

kafka-consumer-groups.sh --bootstrap-server kafka1:9092 --group app-message-closures --delete
kafka-consumer-groups.sh --bootstrap-server kafka1:9092 --group transaction-test --delete

cat seed_data/consent.Client.txt | kafka-console-producer.sh --bootstrap-server kafka1:9092 --topic consent.Client --property parse.key=true --property key.separator=:
cat seed_data/changeOfAddress.txt | kafka-console-producer.sh --bootstrap-server kafka1:9092 --topic changeOfAddress --property parse.key=true --property key.separator=: