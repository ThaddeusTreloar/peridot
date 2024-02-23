#!/bin/sh

#cat consent.Client.txt | while read line; 
#do
#    echo $line | kafka-console-producer --bootstrap-server domain-a.com:29092 --topic consent.Client --property parse.key=true --property key.separator=:
#    echo $line
#done

for i in {1..100};
do
    cat consent.Client.txt | kafka-console-producer --bootstrap-server domain-a.com:9092 --topic consent.Client --property parse.key=true --property key.separator=:
done