import tweepy
import json
import os
from datetime import datetime
import logging
from typing import List, Dict
import time

class TwitterStreamer:
    def __init__(self, config: Dict):
        self.config = config
        self.setup_logging()
        self.setup_twitter_api()
        
    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
    def setup_twitter_api(self):
        """Initialize Twitter API client"""
        try:
            self.client = tweepy.Client(
                bearer_token=os.getenv('TWITTER_BEARER_TOKEN'),
                consumer_key=os.getenv('TWITTER_CONSUMER_KEY'),
                consumer_secret=os.getenv('TWITTER_CONSUMER_SECRET'),
                access_token=os.getenv('TWITTER_ACCESS_TOKEN'),
                access_token_secret=os.getenv('TWITTER_ACCESS_TOKEN_SECRET'),
                wait_on_rate_limit=True
            )
            self.logger.info("Twitter API client initialized successfully")
        except Exception as e:
            self.logger.error(f"Error initializing Twitter API: {e}")
            
    def search_tweets(self, query: str, max_results: int = 100) -> List[Dict]:
        """Search for tweets based on query"""
        tweets_data = []
        try:
            tweets = self.client.search_recent_tweets(
                query=query,
                max_results=max_results,
                tweet_fields=['created_at', 'author_id', 'public_metrics', 'lang']
            )
            
            if tweets.data:
                for tweet in tweets.data:
                    tweet_data = {
                        'id': tweet.id,
                        'text': tweet.text,
                        'created_at': tweet.created_at.isoformat(),
                        'author_id': tweet.author_id,
                        'lang': tweet.lang,
                        'retweet_count': tweet.public_metrics['retweet_count'],
                        'like_count': tweet.public_metrics['like_count'],
                        'reply_count': tweet.public_metrics['reply_count'],
                        'quote_count': tweet.public_metrics['quote_count'],
                        'collected_at': datetime.now().isoformat()
                    }
                    tweets_data.append(tweet_data)
                    
            self.logger.info(f"Collected {len(tweets_data)} tweets")
            return tweets_data
            
        except Exception as e:
            self.logger.error(f"Error searching tweets: {e}")
            return []
            
    def save_tweets_to_file(self, tweets: List[Dict], filename: str):
        """Save tweets to JSON file"""
        try:
            os.makedirs('data/raw', exist_ok=True)
            filepath = f"data/raw/{filename}"
            
            with open(filepath, 'w', encoding='utf-8') as f:
                for tweet in tweets:
                    f.write(json.dumps(tweet) + '\n')
                    
            self.logger.info(f"Saved {len(tweets)} tweets to {filepath}")
            
        except Exception as e:
            self.logger.error(f"Error saving tweets: {e}")
            
    def continuous_collection(self, keywords: List[str], interval: int = 300):
        """Continuously collect tweets"""
        query = " OR ".join(keywords)
        
        while True:
            try:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                tweets = self.search_tweets(query)
                
                if tweets:
                    filename = f"tweets_{timestamp}.json"
                    self.save_tweets_to_file(tweets, filename)
                    
                self.logger.info(f"Waiting {interval} seconds for next collection...")
                time.sleep(interval)
                
            except KeyboardInterrupt:
                self.logger.info("Collection stopped by user")
                break
            except Exception as e:
                self.logger.error(f"Error in continuous collection: {e}")
                time.sleep(60)  # Wait 1 minute before retrying