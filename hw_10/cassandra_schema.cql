CREATE KEYSPACE IF NOT EXISTS wikipedia
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE wikipedia;

CREATE TABLE IF NOT EXISTS page_creations (
    user_id     text,
    domain      text,
    created_at  text,
    page_title  text,
    PRIMARY KEY ((domain), created_at, user_id)
) WITH CLUSTERING ORDER BY (created_at DESC, user_id ASC);