#!/bin/sh
# Continuously cycles Kafka availability while tests are runnning.
while true; do
  killCleanupRestart.sh
  sleep 2m
  cleanup.sh
  sleep 1m
  devKafka.sh
  sleep 1m
done
