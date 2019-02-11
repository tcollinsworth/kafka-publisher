#!/bin/bash

ps aux | egrep -e 'kafka-console-consumer.sh' | grep -v grep | awk '{print $2}' | xargs kill -9
ps aux | egrep -e 'kafka-console-producer.sh' | grep -v grep | awk '{print $2}' | xargs kill -9
ps aux | egrep -e 'kafka/config/server.properties' | grep -v grep | awk '{print $2}' | xargs kill -9
