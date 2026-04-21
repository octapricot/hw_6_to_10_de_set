#!/bin/bash

docker run --rm \
  --network hw_9_hw9_network \
  -e KAFKA_BROKER=kafka:29092 \
  -e KAFKA_TOPIC=tweets \
  -e OUTPUT_DIR=/output \
  -v "$(pwd)/output:/output" \
  hw9_consumer