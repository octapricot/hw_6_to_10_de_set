import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, BooleanType

KAFKA_BROKER          = os.environ.get("KAFKA_BROKER", "kafka:29092")
KAFKA_INPUT_TOPIC     = os.environ.get("KAFKA_INPUT_TOPIC", "input")
KAFKA_PROCESSED_TOPIC = os.environ.get("KAFKA_PROCESSED_TOPIC", "processed")

ALLOWED_DOMAINS = ["en.wikipedia.org", "www.wikidata.org", "commons.wikimedia.org"]

# Spark Session 

spark = SparkSession.builder \
    .appName("WikipediaFilter") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("Spark session created!")

# Schema

schema = StructType([
    StructField("user_id",     StringType(),  True),
    StructField("user_is_bot", BooleanType(), True),
    StructField("domain",      StringType(),  True),
    StructField("page_title",  StringType(),  True),
    StructField("created_at",  StringType(),  True)
])

# Streaming

df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_INPUT_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# JSON Parsing

df_parsed = df_raw.select(
    F.from_json(
        F.col("value").cast("string"),
        schema
    ).alias("data")
).select("data.*")

# Filtering

df_filtered = df_parsed \
    .filter(F.col("domain").isin(ALLOWED_DOMAINS)) \
    .filter(F.col("user_is_bot") == False)

# Writing to Kafka 

df_output = df_filtered.select(
    F.to_json(F.struct("user_id", "domain", "page_title", "created_at")).alias("value")
)

query = df_output.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("topic", KAFKA_PROCESSED_TOPIC) \
    .option("checkpointLocation", "/tmp/checkpoint/job1") \
    .start()

print(f"Job 1 running! Filtering input and sending to the processed topic.")
query.awaitTermination()