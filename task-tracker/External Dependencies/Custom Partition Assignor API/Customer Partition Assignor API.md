[[External Dependencies]]

Currently the rdkafka cpp library does not provide public access to the partition assignor API.
There is currently a feature request here https://github.com/confluentinc/librdkafka/issues/2284
and a pending PR here https://github.com/confluentinc/librdkafka/pull/3812 but both seem to be stale, we will have to wait and find out.

There is also a KIP for version 2 of consumer groups here https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol
