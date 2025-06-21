from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
from typing import Dict

class SparkProcessor:
    def __init__(self, config: Dict):
        self.config = config
        self.setup_logging()
        self.setup_spark()
        
    def setup_logging(self):
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
    def setup_spark(self):
        """Initialize Spark Session"""
        try:
            self.spark = SparkSession.builder \
                .appName(self.config['spark']['app_name']) \
                .master(self.config['spark']['master']) \
                .config("spark.executor.memory", self.config['spark']['executor_memory']) \
                .config("spark.driver.memory", self.config['spark']['driver_memory']) \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .enableHiveSupport() \
                .getOrCreate()
                
            self.spark.sparkContext.setLogLevel("WARN")
            self.logger.info("Spark session created successfully")
            
        except Exception as e:
            self.logger.error(f"Error creating Spark session: {e}")
            
    def read_json_files(self, path: str):
        """Read JSON files from path"""
        try:
            df = self.spark.read.option("multiline", "false").json(path)
            self.logger.info(f"Read {df.count()} records from {path}")
            return df
        except Exception as e:
            self.logger.error(f"Error reading JSON files: {e}")
            return None
            
    def process_tweets(self, raw_df):
        """Process raw tweets data"""
        try:
            # Define schema for processed data
            processed_df = raw_df.select(
                col("id").cast("string"),
                col("text"),
                col("created_at"),
                col("author_id").cast("string"),
                col("lang"),
                col("retweet_count").cast("int"),
                col("like_count").cast("int"),
                col("reply_count").cast("int"),
                col("quote_count").cast("int"),
                col("collected_at"),
                # Add processing timestamp
                current_timestamp().alias("processed_at"),
                # Extract date for partitioning
                to_date(col("created_at")).alias("tweet_date"),
                # Extract hour for time-based analysis
                hour(col("created_at")).alias("tweet_hour")
            ).filter(
                # Filter English tweets only
                col("lang") == "en"
            ).filter(
                # Filter out retweets
                ~col("text").startswith("RT @")
            )
            
            self.logger.info(f"Processed {processed_df.count()} tweets")
            return processed_df
            
        except Exception as e:
            self.logger.error(f"Error processing tweets: {e}")
            return None
            
    def add_sentiment_analysis(self, df):
        """Add sentiment analysis using Spark UDF"""
        from textblob import TextBlob
        import re
        
        def analyze_sentiment_udf(text):
            try:
                # Clean text
                cleaned = re.sub(r'http\S+|www\S+|https\S+', '', text, flags=re.MULTILINE)
                cleaned = re.sub(r'@\w+|#\w+', '', cleaned)
                cleaned = ' '.join(cleaned.split()).strip()
                
                # Analyze sentiment
                blob = TextBlob(cleaned)
                polarity = float(blob.sentiment.polarity)
                subjectivity = float(blob.sentiment.subjectivity)
                
                # Classify sentiment
                if polarity > 0.1:
                    sentiment_label = 'positive'
                elif polarity < -0.1:
                    sentiment_label = 'negative'
                else:
                    sentiment_label = 'neutral'
                    
                return (polarity, subjectivity, sentiment_label, abs(polarity))
                
            except:
                return (0.0, 0.0, 'neutral', 0.0)
        
        # Register UDF
        sentiment_udf = udf(analyze_sentiment_udf, 
                           StructType([
                               StructField("polarity", DoubleType()),
                               StructField("subjectivity", DoubleType()),
                               StructField("sentiment_label", StringType()),
                               StructField("confidence", DoubleType())
                           ]))
        
        # Apply sentiment analysis
        result_df = df.withColumn("sentiment_analysis", sentiment_udf(col("text"))) \
                     .select("*",
                            col("sentiment_analysis.polarity").alias("polarity"),
                            col("sentiment_analysis.subjectivity").alias("subjectivity"),
                            col("sentiment_analysis.sentiment_label").alias("sentiment_label"),
                            col("sentiment_analysis.confidence").alias("confidence")) \
                     .drop("sentiment_analysis")
        
        return result_df
        
    def save_to_hdfs(self, df, path: str, mode: str = "append"):
        """Save DataFrame to HDFS"""
        try:
            df.write \
              .mode(mode) \
              .partitionBy("tweet_date") \
              .parquet(path)
            
            self.logger.info(f"Saved data to HDFS: {path}")
            
        except Exception as e:
            self.logger.error(f"Error saving to HDFS: {e}")
            
    def create_aggregated_views(self, df):
        """Create aggregated views for dashboard"""
        try:
            # Hourly sentiment aggregation
            hourly_sentiment = df.groupBy("tweet_date", "tweet_hour", "sentiment_label") \
                                .agg(
                                    count("*").alias("tweet_count"),
                                    avg("polarity").alias("avg_polarity"),
                                    avg("confidence").alias("avg_confidence")
                                )
            
            # Daily sentiment summary
            daily_sentiment = df.groupBy("tweet_date", "sentiment_label") \
                               .agg(
                                   count("*").alias("tweet_count"),
                                   avg("polarity").alias("avg_polarity"),
                                   sum("like_count").alias("total_likes"),
                                   sum("retweet_count").alias("total_retweets")
                               )
            
            # Top trending topics (based on engagement)
            trending_topics = df.filter(col("like_count") > 10) \
                               .groupBy("tweet_date") \
                               .agg(
                                   sum("like_count").alias("total_engagement"),
                                   count("*").alias("tweet_count"),
                                   avg("polarity").alias("avg_sentiment")
                               )
            
            return {
                'hourly_sentiment': hourly_sentiment,
                'daily_sentiment': daily_sentiment,
                'trending_topics': trending_topics
            }
            
        except Exception as e:
            self.logger.error(f"Error creating aggregated views: {e}")
            return None
            
    def stop_spark(self):
        """Stop Spark session"""
        if hasattr(self, 'spark'):
            self.spark.stop()
            self.logger.info("Spark session stopped")