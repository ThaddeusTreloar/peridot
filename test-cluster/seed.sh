#!/bin/sh

cat seed_data/consent.Client.txt | kafka-console-producer.sh --bootstrap-server kafka1:9092 --topic consent.Client --property parse.key=true --property key.separator=:
cat seed_data/changeOfAddress.txt | kafka-console-producer.sh --bootstrap-server kafka1:9092 --topic changeOfAddress --property parse.key=true --property key.separator=:
