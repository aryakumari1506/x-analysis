-- Create Hive database
CREATE DATABASE IF NOT EXISTS sentiment_db;
USE sentiment_db;

-- Raw tweets table
CREATE TABLE IF NOT EXISTS raw_tweets (
    id STRING,
    text STRING,
    created_at TIMESTAMP,
    author_id STRING,
    lang STRING,
    retweet_count INT,
    like_count INT,
    reply_count INT,
    quote_count INT,
    collected_at TIMESTAMP
)
PARTITIONED BY (tweet_date DATE)
STORED AS PARQUET
LOCATION '/sentiment_data/raw_tweets';

-- Processed tweets with sentiment
CREATE TABLE IF NOT EXISTS processed_tweets (
    id STRING,
    text STRING,
    created_at TIMESTAMP,
    author_id STRING,
    lang STRING,
    retweet_count INT,
    like_count INT,
    reply_count INT,
    quote_count INT,
    collected_at TIMESTAMP,
    processed_at TIMESTAMP,
    tweet_hour INT,
    polarity DOUBLE,
    subjectivity DOUBLE,
    sentiment_label STRING,
    confidence DOUBLE
)
PARTITIONED BY (tweet_date DATE)
STORED AS PARQUET
LOCATION '/sentiment_data/processed_tweets';

-- Hourly sentiment aggregation
CREATE TABLE IF NOT EXISTS hourly_sentiment (
    tweet_date DATE,
    tweet_hour INT,
    sentiment_label STRING,
    tweet_count BIGINT,
    avg_polarity DOUBLE,
    avg_confidence DOUBLE
)
STORED AS PARQUET
LOCATION '/sentiment_data/hourly_sentiment';

-- Daily sentiment summary
CREATE TABLE IF NOT EXISTS daily_sentiment (
    tweet_date DATE,
    sentiment_label STRING,
    tweet_count BIGINT,
    avg_polarity DOUBLE,
    total_likes BIGINT,
    total_retweets BIGINT
)
STORED AS PARQUET
LOCATION '/sentiment_data/daily_sentiment';

-- Create views for Power BI
CREATE VIEW IF NOT EXISTS sentiment_trends AS
SELECT 
    tweet_date,
    sentiment_label,
    tweet_count,
    avg_polarity,
    CASE 
        WHEN avg_polarity > 0.3 THEN 'Very Positive'
        WHEN avg_polarity > 0.1 THEN 'Positive'
        WHEN avg_polarity > -0.1 THEN 'Neutral'
        WHEN avg_polarity > -0.3 THEN 'Negative'
        ELSE 'Very Negative'
    END as sentiment_category
FROM daily_sentiment
ORDER BY tweet_date DESC;

-- Hourly trends view
CREATE VIEW IF NOT EXISTS hourly_trends AS
SELECT 
    tweet_date,
    tweet_hour,
    SUM(CASE WHEN sentiment_label = 'positive' THEN tweet_count ELSE 0 END) as positive_count,
    SUM(CASE WHEN sentiment_label = 'negative' THEN tweet_count ELSE 0 END) as negative_count,
    SUM(CASE WHEN sentiment_label = 'neutral' THEN tweet_count ELSE 0 END) as neutral_count,
    SUM(tweet_count) as total_count
FROM hourly_sentiment
GROUP BY tweet_date, tweet_hour
ORDER BY tweet_date DESC, tweet_hour DESC;