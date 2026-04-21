#!/bin/bash

echo "Building producer image ..."
docker build -t hw9_producer ./producer
echo "Producer image built"

echo "Building consumer image ..."
docker build -t hw9_consumer ./consumer
echo "Consumer image built"