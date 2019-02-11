#!/bin/bash

~/kafka/bin/kafka-server-start.sh ~/kafka/config/server.properties &
sleep 10

# start monitor on topic
gnome-terminal --title="kafka consumer" -x sh -c "~/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test-topic --property print.timestamp=true --property print.key=true --from-beginning; bash"

# start production on topic
gnome-terminal --title="kafka producer" -x sh -c "~/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test-topic --property parse.key=true --property key.separator=:; bash"
