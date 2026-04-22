#!/bin/bash

echo "--- HW10 Teardown ---"

echo "Stopping generator ..."
docker stop hw10_generator 2>/dev/null || true
docker rm hw10_generator 2>/dev/null || true

echo "Stopping Spark cluster ..."
docker-compose -f docker-compose.spark.yml down

echo "Stopping Cassandra ..."
docker-compose -f docker-compose.cassandra.yml down

echo "Stopping Kafka ..."
docker-compose -f docker-compose.kafka.yml down

echo "All containers stopped. Thank you for your feedback on this hw, and thank you for the course! :)"