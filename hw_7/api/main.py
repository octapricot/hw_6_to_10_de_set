import json
import logging
from contextlib import asynccontextmanager

import redis.asyncio as aioredis
from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy
from cassandra.query import SimpleStatement
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from pydantic_settings import BaseSettings

# Configuration

class Settings(BaseSettings):
    cassandra_host: str = "cassandra"
    cassandra_port: int = 9042
    cassandra_keyspace: str = "amazon_reviews"
    redis_host: str = "redis"
    redis_port: int = 6379
    redis_ttl: int = 300

    class Config:
        env_file = ".env"

settings = Settings()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Pydantic Response Models

class ReviewResponse(BaseModel):
    review_id: str
    product_id: str
    customer_id: str
    product_title: str
    star_rating: int
    review_headline: str
    review_body: str
    review_date: str

class CustomerReviewResponse(BaseModel):
    review_id: str
    product_id: str
    product_title: str
    star_rating: int
    review_headline: str
    review_body: str
    review_date: str

class TopProductResponse(BaseModel):
    product_id: str
    product_title: str
    review_count: int

class TopCustomerResponse(BaseModel):
    customer_id: str
    review_count: int

# Cassandra Client Class

class CassandraClient:
    def __init__(self):
        self.cluster = None
        self.session = None

    def connect(self):
        self.cluster = Cluster(
            [settings.cassandra_host],
            port=settings.cassandra_port,
            load_balancing_policy=RoundRobinPolicy(),
            protocol_version=5
        )
        self.session = self.cluster.connect(settings.cassandra_keyspace)
        logger.info("Cassandra connection pool established")

    def disconnect(self):
        if self.cluster:
            self.cluster.shutdown()
            logger.info("Cassandra connection closed")

    def execute(self, query: str, params: tuple = None):
        statement = SimpleStatement(query)
        return self.session.execute(statement, params)

# Cache Service Class 

class CacheService:
    def __init__(self):
        self.client = None

    async def connect(self):
        self.client = await aioredis.from_url(
            f"redis://{settings.redis_host}:{settings.redis_port}",
            socket_timeout=2,
            socket_connect_timeout=2
        )
        logger.info("Redis connection established")

    async def disconnect(self):
        if self.client:
            await self.client.close()

    async def get(self, key: str):
        try:
            value = await self.client.get(key)
            return json.loads(value) if value else None
        except Exception as e:
            logger.warning(f"Redis GET failed for key {key}: {e}")
            return None

    async def set(self, key: str, value):
        try:
            await self.client.setex(
                key,
                settings.redis_ttl,
                json.dumps(value)
            )
        except Exception as e:
            logger.warning(f"Redis SET failed for key {key}: {e}")

# App Lifespan

cassandra_client = CassandraClient()
cache_service = CacheService()

@asynccontextmanager
async def lifespan(app: FastAPI):
    cassandra_client.connect()
    await cache_service.connect()
    yield
    cassandra_client.disconnect()
    await cache_service.disconnect()

app = FastAPI(
    title="Amazon Reviews API",
    description="REST API for Amazon Reviews analytics",
    version="1.0.0",
    lifespan=lifespan
)

# Endpoint 1: All reviews for a product

@app.get("/reviews/product/{product_id}", response_model=list[ReviewResponse])
async def get_reviews_by_product(product_id: str):
    cache_key = f"reviews:product:{product_id}"

    cached = await cache_service.get(cache_key)
    if cached:
        logger.info(f"Cache HIT for {cache_key}")
        return cached

    try:
        rows = cassandra_client.execute(
            "SELECT review_id, product_id, customer_id, product_title, "
            "star_rating, review_headline, review_body, review_date "
            "FROM reviews_by_product WHERE product_id = %s",
            (product_id,)
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cassandra error: {str(e)}")

    results = [
        ReviewResponse(
            review_id=row.review_id,
            product_id=row.product_id,
            customer_id=row.customer_id,
            product_title=row.product_title,
            star_rating=row.star_rating,
            review_headline=row.review_headline,
            review_body=row.review_body or "",
            review_date=str(row.review_date)
        ).model_dump()
        for row in rows
    ]

    if not results:
        raise HTTPException(status_code=404, detail="No reviews found for this product")

    await cache_service.set(cache_key, results)
    return results

# Endpoint 2: Reviews for a product by star rating

@app.get(
    "/reviews/product/{product_id}/rating/{star_rating}",
    response_model=list[ReviewResponse]
)
async def get_reviews_by_product_and_rating(product_id: str, star_rating: int):
    if star_rating not in range(1, 6):
        raise HTTPException(status_code=400, detail="star_rating must be between 1 and 5")

    cache_key = f"reviews:product:{product_id}:rating:{star_rating}"

    cached = await cache_service.get(cache_key)
    if cached:
        logger.info(f"Cache HIT for {cache_key}")
        return cached

    try:
        rows = cassandra_client.execute(
            "SELECT review_id, product_id, customer_id, product_title, "
            "star_rating, review_headline, review_body, review_date "
            "FROM reviews_by_product "
            "WHERE product_id = %s AND star_rating = %s",
            (product_id, star_rating)
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cassandra error: {str(e)}")

    results = [
        ReviewResponse(
            review_id=row.review_id,
            product_id=row.product_id,
            customer_id=row.customer_id,
            product_title=row.product_title,
            star_rating=row.star_rating,
            review_headline=row.review_headline,
            review_body=row.review_body or "",
            review_date=str(row.review_date)
        ).model_dump()
        for row in rows
    ]

    if not results:
        raise HTTPException(
            status_code=404,
            detail="No reviews found for this product and rating"
        )

    await cache_service.set(cache_key, results)
    return results

# Endpoint 3: All reviews for a customer

@app.get("/reviews/customer/{customer_id}", response_model=list[CustomerReviewResponse])
async def get_reviews_by_customer(customer_id: str):
    cache_key = f"reviews:customer:{customer_id}"

    cached = await cache_service.get(cache_key)
    if cached:
        logger.info(f"Cache HIT for {cache_key}")
        return cached

    try:
        rows = cassandra_client.execute(
            "SELECT review_id, product_id, product_title, star_rating, "
            "review_headline, review_body, review_date "
            "FROM reviews_by_customer WHERE customer_id = %s",
            (customer_id,)
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cassandra error: {str(e)}")

    results = [
        CustomerReviewResponse(
            review_id=row.review_id,
            product_id=row.product_id,
            product_title=row.product_title,
            star_rating=row.star_rating,
            review_headline=row.review_headline,
            review_body=row.review_body or "",
            review_date=str(row.review_date)
        ).model_dump()
        for row in rows
    ]

    if not results:
        raise HTTPException(status_code=404, detail="No reviews found for this customer")

    await cache_service.set(cache_key, results)
    return results

# Endpoint 4: Top N most reviewed products

@app.get("/top/products", response_model=list[TopProductResponse])
async def get_top_products(
    period: str = Query(..., description="Period in YYYY-MM format, e.g. 2005-10"),
    n: int = Query(10, ge=1, le=100, description="Number of results to return")
):
    cache_key = f"top:products:{period}:{n}"

    cached = await cache_service.get(cache_key)
    if cached:
        logger.info(f"Cache HIT for {cache_key}")
        return cached

    try:
        rows = cassandra_client.execute(
            "SELECT product_id, product_title, review_count "
            "FROM top_reviewed_items_by_period "
            "WHERE period = %s LIMIT %s",
            (period, n)
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cassandra error: {str(e)}")

    results = [
        TopProductResponse(
            product_id=row.product_id,
            product_title=row.product_title,
            review_count=row.review_count
        ).model_dump()
        for row in rows
    ]

    if not results:
        raise HTTPException(status_code=404, detail=f"No data found for period {period}")

    await cache_service.set(cache_key, results)
    return results

# Endpoint 5: Top N most productive customers

@app.get("/top/customers", response_model=list[TopCustomerResponse])
async def get_top_customers(
    period: str = Query(..., description="Period in YYYY-MM format"),
    n: int = Query(10, ge=1, le=100)
):
    cache_key = f"top:customers:{period}:{n}"

    cached = await cache_service.get(cache_key)
    if cached:
        logger.info(f"Cache HIT for {cache_key}")
        return cached

    try:
        rows = cassandra_client.execute(
            "SELECT customer_id, review_count "
            "FROM top_customers_by_period "
            "WHERE period = %s LIMIT %s",
            (period, n)
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cassandra error: {str(e)}")

    results = [
        TopCustomerResponse(
            customer_id=row.customer_id,
            review_count=row.review_count
        ).model_dump()
        for row in rows
    ]

    if not results:
        raise HTTPException(status_code=404, detail=f"No data found for period {period}")

    await cache_service.set(cache_key, results)
    return results

# Endpoint 6: Top N haters

@app.get("/top/haters", response_model=list[TopCustomerResponse])
async def get_top_haters(
    period: str = Query(..., description="Period in YYYY-MM format"),
    n: int = Query(10, ge=1, le=100)
):
    cache_key = f"top:haters:{period}:{n}"

    cached = await cache_service.get(cache_key)
    if cached:
        logger.info(f"Cache HIT for {cache_key}")
        return cached

    try:
        rows = cassandra_client.execute(
            "SELECT customer_id, review_count "
            "FROM top_sentiment_by_period "
            "WHERE period = %s AND sentiment = %s LIMIT %s",
            (period, "hater", n)
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cassandra error: {str(e)}")

    results = [
        TopCustomerResponse(
            customer_id=row.customer_id,
            review_count=row.review_count
        ).model_dump()
        for row in rows
    ]

    if not results:
        raise HTTPException(status_code=404, detail=f"No data found for period {period}")

    await cache_service.set(cache_key, results)
    return results

# Endpoint 7: Top N backers

@app.get("/top/backers", response_model=list[TopCustomerResponse])
async def get_top_backers(
    period: str = Query(..., description="Period in YYYY-MM format"),
    n: int = Query(10, ge=1, le=100)
):
    cache_key = f"top:backers:{period}:{n}"

    cached = await cache_service.get(cache_key)
    if cached:
        logger.info(f"Cache HIT for {cache_key}")
        return cached

    try:
        rows = cassandra_client.execute(
            "SELECT customer_id, review_count "
            "FROM top_sentiment_by_period "
            "WHERE period = %s AND sentiment = %s LIMIT %s",
            (period, "backer", n)
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cassandra error: {str(e)}")

    results = [
        TopCustomerResponse(
            customer_id=row.customer_id,
            review_count=row.review_count
        ).model_dump()
        for row in rows
    ]

    if not results:
        raise HTTPException(status_code=404, detail=f"No data found for period {period}")

    await cache_service.set(cache_key, results)
    return results