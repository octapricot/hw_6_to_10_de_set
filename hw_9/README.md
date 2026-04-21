# Homework 9: Working with Kafka (Reading)

## What this does
Continuously reads messages from a Kafka topic, extracts author_id, created_at, and text fields, and writes them to minute-based CSV files on the local filesystem, simulating real-time tweet stream processing.

## Project structure
- `producer/producer.py`: streams tweets to Kafka (reused from HW8)
- `producer/Dockerfile`: containerizes the producer
- `consumer/consumer.py`: reads from Kafka and writes to CSV files
- `consumer/Dockerfile`: containerizes the consumer
- `docker-compose.yml`: spins up Kafka in KRaft mode
- `build.sh`: builds both producer and consumer images
- `run_producer.sh`: runs the producer container on the Kafka network
- `run_consumer.sh`: runs the consumer container on the Kafka network
- `data/`: place `twcs.csv` here (not tracked by Git)
- `output/`: generated CSV files land here (not tracked by Git)

## Output file format
A new CSV file is created every minute, name convention looks like this: `tweets_dd_mm_yyyy_hh_mm.csv`
Each file contains three columns: `author_id`, `created_at`, `text`

## Setup

### 1. Start Kafka
```bash
docker-compose up -d
```

### 2. Build both images
```bash
./build.sh
```

### 3. Run the producer (Terminal 1)
```bash
./run_producer.sh
```

### 4. Run the consumer (Terminal 2)
```bash
./run_consumer.sh
```

The consumer will create a new CSV file every minute (my hopeful estimation).

## Licence + Disclaimer
None, no data belongs to me, all is being done for education purposes only :)