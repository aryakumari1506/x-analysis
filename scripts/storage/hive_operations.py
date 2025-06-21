from pyhive import hive
import pandas as pd
import logging
from typing import Dict, List

class HiveOperations:
    def __init__(self, config: Dict):
        self.config = config
        self.setup_logging()
        self.connect()
        
    def setup_logging(self):
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
    def connect(self):
        """Connect to Hive"""
        try:
            self.connection = hive.Connection(
                host=self.config['hive']['host'],
                port=self.config['hive']['port'],
                database=self.config['hive']['database']
            )
            self.logger.info("Connected to Hive successfully")
        except Exception as e:
            self.logger.error(f"Error connecting to Hive: {e}")
            
    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute query and return results as DataFrame"""
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            
            # Fetch results
            results = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            
            df = pd.DataFrame(results, columns=columns)
            cursor.close()
            
            self.logger.info(f"Query executed successfully, returned {len(df)} rows")
            return df
            
        except Exception as e:
            self.logger.error(f"Error executing query: {e}")
            return pd.DataFrame()
            
    def get_sentiment_trends(self, days: int = 7) -> pd.DataFrame:
        """Get sentiment trends for last N days"""
        query = f"""
        SELECT 
            tweet_date,
            sentiment_label,
            tweet_count,
            avg_polarity,
            sentiment_category
        FROM sentiment_trends
        WHERE tweet_date >= date_sub(current_date(), {days})
        ORDER BY tweet_date DESC, sentiment_label
        """
        return self.execute_query(query)
        
    def get_hourly_trends(self, date: str) -> pd.DataFrame:
        """Get hourly trends for specific date"""
        query = f"""
        SELECT *
        FROM hourly_trends
        WHERE tweet_date = '{date}'
        ORDER BY tweet_hour
        """
        return self.execute_query(query)
        
    def get_dashboard_summary(self) -> Dict:
        """Get summary statistics for dashboard"""
        queries = {
            'total_tweets': "SELECT COUNT(*) as total FROM processed_tweets",
            'sentiment_distribution': """
                SELECT sentiment_label, COUNT(*) as count 
                FROM processed_tweets 
                WHERE tweet_date >= date_sub(current_date(), 1)
                GROUP BY sentiment_label
            """,
            'avg_sentiment': """
                SELECT AVG(polarity) as avg_sentiment 
                FROM processed_tweets 
                WHERE tweet_date >= date_sub(current_date(), 1)
            """
        }
        
        results = {}
        for key, query in queries.items():
            try:
                df = self.execute_query(query)
                results[key] = df.to_dict('records')
            except Exception as e:
                self.logger.error(f"Error executing query {key}: {e}")
                results[key] = []
                
        return results
        
    def close_connection(self):
        """Close Hive connection"""
        if hasattr(self, 'connection'):
            self.connection.close()
            self.logger.info("Hive connection closed")