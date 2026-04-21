# Homework 8: Working with Kafka (Writing)

## What this does
Reads the Twitter Customer Support dataset from a CSV file and streams it to a Kafka topic as individual messages, simulating a live tweet stream at 10-15 messages per second (hopefully) with current timestamps.

## Project structure
- `producer.py`: reads the CSV and streams messages to Kafka
- `Dockerfile`: containerizes the producer
- `build.sh`: builds the producer container image
- `run.sh`: runs the producer container on the Kafka network
- `docker-compose.yml`: spins up Kafka in KRaft mode (no Zookeeper)
- `data/`: place `twcs.csv` here (not tracked by Git)

## Setup

### 1. Start Kafka
```bash
docker-compose up -d
```

### 2. Build the producer image
```bash
./build.sh
```

### 3. Run the producer
```bash
./run.sh
```

### 4. Verify messages in Kafka
```bash
docker exec -it hw8_kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic tweets \
  --from-beginning \
  --max-messages 10
```

## Licence + Disclaimer
None, no data belongs to me, all is being done for education purposes only :)