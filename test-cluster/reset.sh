#!/bin/bash

kafka-topics.sh --bootstrap-server=kafka1:9092 --topic app-message-closures-consent_table-Changelog --delete
kafka-topics.sh --bootstrap-server=kafka1:9092 --topic app-message-closures-consent_table-Changelog --create --partitions 2
kafka-topics.sh --bootstrap-server=kafka1:9092 --topic genericTopic --delete
kafka-topics.sh --bootstrap-server=kafka1:9092 --topic genericTopic --create --partitions 2
kafka-consumer-groups.sh --bootstrap-server kafka1:9092 --group app-message-closures --delete
kafka-consumer-groups.sh --bootstrap-server kafka1:9092 --group transaction-test --delete