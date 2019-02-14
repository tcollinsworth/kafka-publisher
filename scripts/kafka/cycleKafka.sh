#!/bin/sh
# Continuously cycles Kafka availability while tests are runnning.
while true; do
  killCleanupRestart.sh
  sleep 1m
  killKafka.sh
  sleep 10s
  startKafka.sh
  sleep 1m
  rm ../../__integ-tests__/kafkaFallbackLogs/*.log
  sleep 1s
done
