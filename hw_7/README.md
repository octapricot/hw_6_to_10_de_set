# Homework 7: Complex Analytics with Apache Spark, Cassandra, Redis and FastAPI

## What this does
Transforms and loads Amazon reviews data using PySpark into Cassandra, then serves the data through a REST API with Redis caching.

## Project structure
- `pyspark_cassandra.py`: main PySpark script
- `cassandra_schema.cql`: Cassandra keyspace and table definitions
- `docker-compose.yml`: spins up Cassandra, Redis, and the API
- `api/main.py`: FastAPI application with all 7 endpoints
- `api/Dockerfile`: containerizes the API
- `api/requirements.txt`: Python dependencies
- `data/`: place `amazon_reviews.csv` here (not tracked by Git) 

## Setup

### 1. Create your `.env` file
```
CASSANDRA_HOST=cassandra
CASSANDRA_PORT=9042
CASSANDRA_KEYSPACE=amazon_reviews
REDIS_HOST=redis
REDIS_PORT=6379
REDIS_TTL=300
```

### 2. Start all services
```bash
docker-compose up -d
```

### 3. Load Cassandra schema
```bash
docker exec -i hw7_cassandra cqlsh < cassandra_schema.cql
```

### 4. Run the pipeline
```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@17
export SPARK_HOME=/opt/homebrew/opt/apache-spark/libexec
export CASSANDRA_HOST=localhost
export CASSANDRA_PORT=9043
export CASSANDRA_KEYSPACE=amazon_reviews
python3 pyspark_cassandra.py
```

## API Endpoints
| Method | Endpoint | Description |
|---|---|---|
| GET | `/reviews/product/{product_id}` | All reviews for a product |
| GET | `/reviews/product/{product_id}/rating/{star_rating}` | Reviews for a product filtered by star rating |
| GET | `/reviews/customer/{customer_id}` | All reviews by a customer |
| GET | `/top/products?period=YYYY-MM&n=N` | Top N most reviewed products in a period |
| GET | `/top/customers?period=YYYY-MM&n=N` | Top N most active customers in a period |
| GET | `/top/haters?period=YYYY-MM&n=N` | Top N customers with most 1-2 star reviews |
| GET | `/top/backers?period=YYYY-MM&n=N` | Top N customers with most 4-5 star reviews | 

Interactive docs available at `http://localhost:8000/docs`

## Licence + Disclaimer
None, no data in reviews belongs to me, all is being done for education purposes only :)