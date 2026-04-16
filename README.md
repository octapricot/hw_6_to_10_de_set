# Homework 6: Data Processing with Apache Spark and MongoDB 

## What this does
Loads Amazon product reviews from a CSV, cleans and aggregates the data using Apache Spark, and stores the results in MongoDB.

## Project structure
- `pyspark_amazon.py`: main PySpark script
- `docker-compose.yml`: spins up MongoDB locally
- `data/`: place `amazon_reviews.csv` here 

## Setup

### 1. Create your `.env` file

```
MONGO_USERNAME=admin
MONGO_PASSWORD=your_password
MONGO_DATABASE=amazon_reviews
MONGO_PORT=27018
```

### 2. Start MongoDB
```bash
docker-compose up -d
```

### 3. Run the pipeline
```bash
python3 pyspark_amazon.py
```

## Results stored in MongoDB
- `product_stats`: total reviews and average rating per product
- `customer_stats`: verified review count per customer
- `monthly_product_reviews`: monthly review trends per product

## Licence + Disclaimer

None, no data in reviews belongs to me, all is being done for the education purposes only :)