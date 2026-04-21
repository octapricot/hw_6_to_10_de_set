import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CASSANDRA_HOST  = os.environ.get("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT  = os.environ.get("CASSANDRA_PORT", "9043")
CASSANDRA_KEYSPACE = os.environ.get("CASSANDRA_KEYSPACE", "amazon_reviews")
CSV_PATH = "../data/amazon_reviews.csv"

# Spark Session 

spark = SparkSession.builder \
    .appName("AmazonReviewsCassandra") \
    .config(
        "spark.jars.packages",
        "com.datastax.spark:spark-cassandra-connector_2.13:3.5.1"
    ) \
    .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
    .config("spark.cassandra.connection.port", CASSANDRA_PORT) \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("Spark session created")

# Load and Clean CSV (reusing the csv from hw6)

df_raw = spark.read.csv(
    CSV_PATH,
    header=True,
    inferSchema=False,
    quote='"',
    escape='"',
    multiLine=True
)

df = df_raw \
    .withColumn("star_rating",      F.col("star_rating").cast("integer")) \
    .withColumn("verified_purchase",F.col("verified_purchase").cast("integer")) \
    .withColumn("helpful_votes",    F.col("helpful_votes").cast("integer")) \
    .withColumn("total_votes",      F.col("total_votes").cast("integer")) \
    .withColumn("review_date",      F.to_date(F.col("review_date"), "yyyy-MM-dd"))

df = df.dropna(subset=["review_id", "product_id", "star_rating", "review_date"])
df = df.filter(F.col("verified_purchase") == 1)

df = df.withColumn("period", F.date_format(F.col("review_date"), "yyyy-MM"))

print(f"Data cleaned: {df.count()} rows")

# Helper: Write to Cassandra 

def write_to_cassandra(df, table):
    df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table=table, keyspace=CASSANDRA_KEYSPACE) \
        .save()
    print(f"Written to Cassandra table: {table}")

# Table 1: reviews_by_product 

df_by_product = df.select(
    "product_id",
    "star_rating",
    "review_date",
    "review_id",
    "customer_id",
    "product_title",
    "review_headline",
    "review_body"
)

write_to_cassandra(df_by_product, "reviews_by_product")

# Table 2: reviews_by_customer 

df_by_customer = df.select(
    "customer_id",
    "review_date",
    "review_id",
    "product_id",
    "product_title",
    "star_rating",
    "review_headline",
    "review_body"
)

write_to_cassandra(df_by_customer, "reviews_by_customer")

# Table 3: top_reviewed_items_by_period 

df_top_products = df \
    .groupBy("period", "product_id", "product_title") \
    .agg(F.count("review_id").alias("review_count"))

write_to_cassandra(df_top_products, "top_reviewed_items_by_period")

# Table 4: top_customers_by_period 

df_top_customers = df \
    .groupBy("period", "customer_id") \
    .agg(F.count("review_id").alias("review_count"))

write_to_cassandra(df_top_customers, "top_customers_by_period")

# Table 5: top_sentiment_by_period 

df_haters = df.filter(F.col("star_rating") <= 2) \
    .groupBy("period", "customer_id") \
    .agg(F.count("review_id").alias("review_count")) \
    .withColumn("sentiment", F.lit("hater"))

df_backers = df.filter(F.col("star_rating") >= 4) \
    .groupBy("period", "customer_id") \
    .agg(F.count("review_id").alias("review_count")) \
    .withColumn("sentiment", F.lit("backer"))

df_sentiment = df_haters.union(df_backers)

write_to_cassandra(df_sentiment, "top_sentiment_by_period")

spark.stop()
print("All done. Does it work?")