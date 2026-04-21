#!/bin/bash
docker run --rm \
  --network hw_9_hw9_network \
  -e KAFKA_BROKER=kafka:29092 \
  -e KAFKA_TOPIC=tweets \
  -e CSV_PATH=/data/twcs.csv \
  -v "$(pwd)/data:/data" \
  hw9_producer