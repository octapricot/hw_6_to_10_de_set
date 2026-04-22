# Homework 10: Processing Event Streams with Spark Streaming, Kafka and Cassandra

## What this does
Reads a real-time Wikipedia page creation stream, filters it using Spark Streaming, and stores results in Cassandra.

## Filtering logic

Only messages where:
- `domain` is one of: `en.wikipedia.org`, `www.wikidata.org`, `commons.wikimedia.org`
- `user_is_bot` is `false`

## Project structure
- `generator/`: reads Wikipedia SSE stream, sends events to Kafka
- `spark/job1_filter.py`: Spark Streaming, filters input and sends the filtered to processed topic
- `spark/job2_to_cassandra.py`: Spark Streaming, processed topic streams into Cassandra
- `docker-compose.kafka.yml`: Kafka in KRaft mode
- `docker-compose.cassandra.yml`: single-node Cassandra
- `docker-compose.spark.yml`: Spark master + 2 workers (so that Spark jobs aren't fighting like school kids over a toy)
- `cassandra_schema.cql`: keyspace and table definition
- `build.sh`: builds the generator Docker image
- `setup.sh`: starts all services, creates topics, loads schema, copies jobs
- `teardown.sh`: stops and removes all containers 

## Setup

### 1. Build the generator image
```bash
./build.sh
```

### 2. Run full setup (Kafka, Cassandra, and Spark)
```bash
./setup.sh
```

### 3. Run the pipeline in separate terminals

**Terminal 1 is for Generator:**
```bash
docker run --rm --name hw10_generator \
  --network hw_10_hw10_network \
  -e KAFKA_BROKER=kafka:29092 \
  -e KAFKA_INPUT_TOPIC=input \
  hw10_generator
```

**Terminal 2 is for Spark Job 1 (filter):**
```bash
docker exec \
  -e KAFKA_BROKER=kafka:29092 \
  -e KAFKA_INPUT_TOPIC=input \
  -e KAFKA_PROCESSED_TOPIC=processed \
  hw10_spark_master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --total-executor-cores 1 \
  --executor-memory 512m \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  /opt/spark/work-dir/job1_filter.py
```

**Terminal 3 is for Spark Job 2 (to Cassandra):**
```bash
docker exec \
  -e KAFKA_BROKER=kafka:29092 \
  -e KAFKA_PROCESSED_TOPIC=processed \
  -e CASSANDRA_HOST=cassandra \
  -e CASSANDRA_PORT=9042 \
  -e CASSANDRA_KEYSPACE=wikipedia \
  hw10_spark_master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --total-executor-cores 1 \
  --executor-memory 512m \
  --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1" \
  /opt/spark/work-dir/job2_to_cassandra.py
```

### 4. Verify results (screenshots available)

```bash
# Check Kafka topics
docker exec -it hw10_kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 --topic input --from-beginning --max-messages 5

docker exec -it hw10_kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 --topic processed --from-beginning --max-messages 5

# Check Cassandra
docker exec -it hw10_cassandra cqlsh -e \
  "SELECT * FROM wikipedia.page_creations LIMIT 10;"
```

### 5. Teardown
```bash
./teardown.sh
```

## Licence + Disclaimer
None, no data belongs to me, all is being done for education purposes only :) 