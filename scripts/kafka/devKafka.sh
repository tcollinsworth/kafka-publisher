#!/bin/bash

# to clean up all data and logs and restart fresh call cleanup.sh script
# stops kafka and zookeeper
# goes to kafka dir and deletes logs dir and tmp dir and delete kafka-logs and zookeeper dirs

# development script
# assumes kafka is installed in home dir
# assumes delete topic in config/server.properties is enabled - for dev, not production
# symlink to kafka, i.e., ln -s kafka_2.12-0.10.2.0/ kafka
~/kafka/bin/zookeeper-server-start.sh ~/kafka/config/zookeeper.properties &
sleep 5

~/kafka/bin/kafka-server-start.sh ~/kafka/config/server.properties &
sleep 10

# delete topic to remove old messages
~/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic "test-topic"

# create topic
~/kafka/bin/kafka-topics.sh --create --if-not-exists --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --config message.timestamp.type=LogAppendTime --config retention.bytes=100000000 --config retention.ms=600000 --config segment.bytes=10000000 --config segment.ms=3600000 --topic "test-topic"

# start monitor on topic
gnome-terminal --title="kafka consumer" -x sh -c "~/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test-topic --property print.timestamp=true --property print.key=true --from-beginning; bash"

# start production on topic
gnome-terminal --title="kafka producer" -x sh -c "~/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test-topic --property parse.key=true --property key.separator=:; bash"
