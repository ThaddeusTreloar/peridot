#!/bin/sh

for x in $( seq 1 275 )
do
    cat seed_data/benchData.txt >> ./bd.txt
done

kafka-topics.sh --bootstrap-server=kafka1:9092 --delete --topic inputTopic
kafka-topics.sh --bootstrap-server=kafka1:9092 --delete --topic outputTopic

kafka-topics.sh --bootstrap-server=kafka1:9092 --create --topic inputTopic --partitions 12
kafka-topics.sh --bootstrap-server=kafka1:9092 --create --topic outputTopic --partitions 12

kafka-consumer-groups.sh --bootstrap-server kafka1:9092 --group app-message-bench --delete
cat ./bd.txt | kafka-console-producer.sh --bootstrap-server kafka1:9092 --topic inputTopic --property parse.key=true --property key.separator=:

cargo run --bin app-message-bench --release 

rm ./bd.txt