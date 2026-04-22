import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

KAFKA_BROKER          = os.environ.get("KAFKA_BROKER", "kafka:29092")
KAFKA_PROCESSED_TOPIC = os.environ.get("KAFKA_PROCESSED_TOPIC", "processed")
CASSANDRA_HOST        = os.environ.get("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT        = os.environ.get("CASSANDRA_PORT", "9042")
CASSANDRA_KEYSPACE    = os.environ.get("CASSANDRA_KEYSPACE", "wikipedia")

# Spark Session 

spark = SparkSession.builder \
    .appName("WikipediaToCassandra") \
    .config(
        "spark.jars.packages",
        ",".join([
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
            "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1"
        ])
    ) \
    .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
    .config("spark.cassandra.connection.port", CASSANDRA_PORT) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("Spark session created.")

# Schema

schema = StructType([
    StructField("user_id",    StringType(), True),
    StructField("domain",     StringType(), True),
    StructField("page_title", StringType(), True),
    StructField("created_at", StringType(), True)
])

# Reading from the processed topic

df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_PROCESSED_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

df_parsed = df_raw.select(
    F.from_json(
        F.col("value").cast("string"),
        schema
    ).alias("data")
).select("data.*")

# Writing to Cassandra

def write_to_cassandra(batch_df, batch_id):
    if batch_df.count() == 0:
        return
    batch_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(
            table="page_creations",
            keyspace=CASSANDRA_KEYSPACE
        ) \
        .save()
    print(f"Batch {batch_id} has been written to Cassandra: {batch_df.count()} rows")

query = df_parsed.writeStream \
    .foreachBatch(write_to_cassandra) \
    .option("checkpointLocation", "/tmp/checkpoint/job2") \
    .start()

print("Job 2 is running: writing processed topic to Cassandra.")
query.awaitTermination()