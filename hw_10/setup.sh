#!/bin/bash
set -e

echo "----- HW10 Setup ------"

echo "(1) Starting Kafka ..."
docker-compose -f docker-compose.kafka.yml up -d
echo "   Waiting for Kafka to be ready ..."
sleep 10

echo "(2) Creating Kafka topics ..."
docker exec hw10_kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --create --if-not-exists \
  --topic input \
  --partitions 1 \
  --replication-factor 1

docker exec hw10_kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --create --if-not-exists \
  --topic processed \
  --partitions 1 \
  --replication-factor 1
echo "   Topics created! Yay!"

echo "(3) Starting Cassandra ..."
docker-compose -f docker-compose.cassandra.yml up -d
echo "   Waiting for Cassandra to be healthy ..."
until docker exec hw10_cassandra cqlsh -e "describe keyspaces" > /dev/null 2>&1; do
  sleep 5
  echo "   Still waiting ..."
done
echo "   Cassandra is ready!"

echo "(4) Loading Cassandra schema ..."
docker exec -i hw10_cassandra cqlsh < cassandra_schema.cql
echo "   Schema loaded! Two more steps left."

echo "(5) Starting Spark cluster ..."
docker-compose -f docker-compose.spark.yml up -d
echo "   Waiting for Spark to be ready..."
sleep 15

echo "(6) Fixing Spark permissions ..."
docker exec -u root hw10_spark_master mkdir -p /home/spark/.ivy2/cache
docker exec -u root hw10_spark_master chown -R spark:spark /home/spark/.ivy2

echo "(7) Copying Spark jobs to master ..."
docker cp spark/job1_filter.py hw10_spark_master:/opt/spark/work-dir/
docker cp spark/job2_to_cassandra.py hw10_spark_master:/opt/spark/work-dir/

echo ""
echo "Setup complete!"
echo ""
echo "Now run these in separate terminals:"
echo ""
echo "Terminal 1 is for Generator:"
echo "  docker run --rm --name hw10_generator \\"
echo "    --network hw_10_hw10_network \\"
echo "    -e KAFKA_BROKER=kafka:29092 \\"
echo "    -e KAFKA_INPUT_TOPIC=input \\"
echo "    hw10_generator"
echo ""
echo "Terminal 2 is for Spark Job 1 (filter):"
echo "  docker exec \\"
echo "    -e KAFKA_BROKER=kafka:29092 \\"
echo "    -e KAFKA_INPUT_TOPIC=input \\"
echo "    -e KAFKA_PROCESSED_TOPIC=processed \\"
echo "    hw10_spark_master /opt/spark/bin/spark-submit \\"
echo "    --master spark://spark-master:7077 \\"
echo "    --total-executor-cores 1 \\"
echo "    --executor-memory 512m \\"
echo "    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \\"
echo "    /opt/spark/work-dir/job1_filter.py"
echo ""
echo "Terminal 3 is for Spark Job 2 (to Cassandra):"
echo "  docker exec \\"
echo "    -e KAFKA_BROKER=kafka:29092 \\"
echo "    -e KAFKA_PROCESSED_TOPIC=processed \\"
echo "    -e CASSANDRA_HOST=cassandra \\"
echo "    -e CASSANDRA_PORT=9042 \\"
echo "    -e CASSANDRA_KEYSPACE=wikipedia \\"
echo "    hw10_spark_master /opt/spark/bin/spark-submit \\"
echo "    --master spark://spark-master:7077 \\"
echo "    --total-executor-cores 1 \\"
echo "    --executor-memory 512m \\"
echo "    --packages \"org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1\" \\"
echo "    /opt/spark/work-dir/job2_to_cassandra.py"