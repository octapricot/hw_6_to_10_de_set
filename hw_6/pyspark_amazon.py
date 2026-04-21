import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

MONGO_USERNAME = os.environ.get("MONGO_USERNAME")
MONGO_PASSWORD = os.environ.get("MONGO_PASSWORD")
MONGO_DATABASE = os.environ.get("MONGO_DATABASE")
MONGO_PORT     = os.environ.get("MONGO_PORT", "27018")

MONGO_URI = f"mongodb://{MONGO_USERNAME}:{MONGO_PASSWORD}@localhost:{MONGO_PORT}/"

spark = SparkSession.builder \
    .appName("AmazonReviewsProcessor") \
    .config(
        "spark.jars.packages",
        "org.mongodb.spark:mongo-spark-connector_2.13:11.0.0"
    ) \
    .config("spark.mongodb.write.connection.uri", MONGO_URI) \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("Spark session created successfully")

CSV_PATH = "data/amazon_reviews.csv"

df_raw = spark.read.csv(
    CSV_PATH,
    header=True,
    inferSchema=False,
    quote='"',
    escape='"',
    multiLine=True
)

print(f"CSV loaded; total rows: {df_raw.count()}")
print("Schema:")
df_raw.printSchema()


# Data Cleaning

df_raw = df_raw \
    .withColumn("star_rating", F.col("star_rating").cast("integer")) \
    .withColumn("verified_purchase", F.col("verified_purchase").cast("integer")) \
    .withColumn("helpful_votes", F.col("helpful_votes").cast("integer")) \
    .withColumn("total_votes", F.col("total_votes").cast("integer"))

df_clean = df_raw.dropna(
    subset=["review_id", "product_id", "star_rating", "review_date"]
)

df_clean = df_clean.withColumn(
    "review_date",
    F.to_date(F.col("review_date"), "yyyy-MM-dd")
)

df_clean = df_clean.filter(F.col("verified_purchase") == 1)

print(f"Data cleaned; rows after cleaning: {df_clean.count()}")


# Aggregation 1: Stats per Product

df_product_stats = df_clean \
    .groupBy("product_id", "product_title", "product_category") \
    .agg(
        F.count("review_id").alias("total_reviews"),
        F.round(F.avg("star_rating"), 2).alias("avg_star_rating")
    )

print(f"Aggregation 1 done; unique products: {df_product_stats.count()}")


# Aggregation 2: Verified Reviews per Customer
# As we already filtered for verified_purchase == 1, every row here is already a verified review

df_customer_stats = df_clean \
    .groupBy("customer_id") \
    .agg(
        F.count("review_id").alias("verified_review_count")
    )

print(f"Aggregation 2 done; unique customers: {df_customer_stats.count()}")


# Aggregation 3: Monthly Reviews per Product

df_monthly = df_clean \
    .withColumn("year", F.year("review_date")) \
    .withColumn("month", F.month("review_date")) \
    .groupBy("product_id", "product_title", "year", "month") \
    .agg(
        F.count("review_id").alias("monthly_review_count")
    ) \
    .orderBy("product_id", "year", "month")

print(f"Aggregation 3 done; monthly records: {df_monthly.count()}")


# Writing Results to MongoDB
# Each aggregation goes into its own MongoDB collection

def write_to_mongo(df, collection_name):
    df.write \
        .format("mongodb") \
        .mode("overwrite") \
        .option("database", MONGO_DATABASE) \
        .option("collection", collection_name) \
        .save()
    print(f"Saved to MongoDB collection: '{collection_name}'")

write_to_mongo(df_product_stats, "product_stats")
write_to_mongo(df_customer_stats, "customer_stats")
write_to_mongo(df_monthly, "monthly_product_reviews")

# Creating Indexes in MongoDB

from pymongo import MongoClient

client = MongoClient(MONGO_URI)
db = client[MONGO_DATABASE]

db["product_stats"].create_index("product_id")

db["customer_stats"].create_index("customer_id")

# For trend queries
db["monthly_product_reviews"].create_index(
    [("product_id", 1), ("year", 1), ("month", 1)]
)

print("MongoDB indexes created")

client.close()
spark.stop()
print("Done!")