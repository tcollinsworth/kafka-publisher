#!/bin/bash

#kill consumers, kafka, zookeeper, and cleanup all topics and logs

ps aux | egrep -e 'ConsoleConsumer' | grep -v grep | awk '{print $2}' | xargs kill -9
ps aux | egrep -e 'kafka-console-consumer.sh' | grep -v grep | awk '{print $2}' | xargs kill -9
ps aux | egrep -e 'kafka-console-producer.sh' | grep -v grep | awk '{print $2}' | xargs kill -9
ps aux | egrep -e 'kafka/config/server.properties' | grep -v grep | awk '{print $2}' | xargs kill -9
ps aux | egrep -e 'config/zoo' | grep -v grep | awk '{print $2}' | xargs kill -9

rm -r ~/kafka/logs
rm -r /tmp/kafka-logs
rm -r /tmp/zookeeper
