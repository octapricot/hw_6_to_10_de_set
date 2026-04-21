CREATE KEYSPACE IF NOT EXISTS amazon_reviews
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE amazon_reviews;

-- Table 1: reviews by product
CREATE TABLE IF NOT EXISTS reviews_by_product (
    product_id      text,
    star_rating     int,
    review_date     date,
    review_id       text,
    customer_id     text,
    product_title   text,
    review_headline text,
    review_body     text,
    PRIMARY KEY ((product_id), star_rating, review_date, review_id)
) WITH CLUSTERING ORDER BY (star_rating DESC, review_date DESC, review_id ASC);

-- Table 2: reviews by customer
CREATE TABLE IF NOT EXISTS reviews_by_customer (
    customer_id     text,
    review_date     date,
    review_id       text,
    product_id      text,
    product_title   text,
    star_rating     int,
    review_headline text,
    review_body     text,
    PRIMARY KEY ((customer_id), review_date, review_id)
) WITH CLUSTERING ORDER BY (review_date DESC, review_id ASC);

-- Table 3: top reviewed items by period
CREATE TABLE IF NOT EXISTS top_reviewed_items_by_period (
    period          text,
    review_count    bigint,
    product_id      text,
    product_title   text,
    PRIMARY KEY ((period), review_count, product_id)
) WITH CLUSTERING ORDER BY (review_count DESC, product_id ASC);

-- Table 4: top customers by period
CREATE TABLE IF NOT EXISTS top_customers_by_period (
    period          text,
    review_count    bigint,
    customer_id     text,
    PRIMARY KEY ((period), review_count, customer_id)
) WITH CLUSTERING ORDER BY (review_count DESC, customer_id ASC);

-- Table 5: top sentiment by period
CREATE TABLE IF NOT EXISTS top_sentiment_by_period (
    period          text,
    sentiment       text,
    review_count    bigint,
    customer_id     text,
    PRIMARY KEY ((period, sentiment), review_count, customer_id)
) WITH CLUSTERING ORDER BY (review_count DESC, customer_id ASC);