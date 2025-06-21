import pandas as pd
import pyodbc
import sqlalchemy
from sqlalchemy import create_engine
import logging
from typing import Dict

class PowerBIConnector:
    def __init__(self, config: Dict):
        self.config = config
        self.setup_logging()
        
    def setup_logging(self):
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
    def export_to_csv(self, hive_ops):
        """Export data to CSV files for Power BI"""
        try:
            # Create dashboard data directory
            import os
            os.makedirs('data/dashboard', exist_ok=True)
            
            # Export sentiment trends
            trends_df = hive_ops.get_sentiment_trends(days=30)
            trends_df.to_csv('data/dashboard/sentiment_trends.csv', index=False)
            
            # Export hourly trends for last 7 days
            from datetime import datetime, timedelta
            end_date = datetime.now()
            
            all_hourly_data = []
            for i in range(7):
                date = (end_date - timedelta(days=i)).strftime('%Y-%m-%d')
                hourly_df = hive_ops.get_hourly_trends(date)
                if not hourly_df.empty:
                    all_hourly_data.append(hourly_df)
            
            if all_hourly_data:
                combined_hourly = pd.concat(all_hourly_data, ignore_index=True)
                combined_hourly.to_csv('data/dashboard/hourly_trends.csv', index=False)
            
            # Export summary statistics
            summary = hive_ops.get_dashboard_summary()
            
            # Convert summary to DataFrame format
            summary_records = []
            for key, value in summary.items():
                if isinstance(value, list) and value:
                    for record in value:
                        record['metric_type'] = key
                        summary_records.append(record)
            
            if summary_records:
                summary_df = pd.DataFrame(summary_records)
                summary_df.to_csv('data/dashboard/summary_stats.csv', index=False)
            
            self.logger.info("Data exported to CSV files for Power BI")
            
        except Exception as e:
            self.logger.error(f"Error exporting data to CSV: {e}")
            
    def create_power_bi_dataset(self, hive_ops):
        """Create comprehensive dataset for Power BI"""
        try:
            # Main sentiment data
            sentiment_query = """
            SELECT 
                tweet_date,
                tweet_hour,
                sentiment_label,
                COUNT(*) as tweet_count,
                AVG(polarity) as avg_polarity,
                AVG(confidence) as avg_confidence,
                SUM(like_count) as total_likes,
                SUM(retweet_count) as total_retweets
            FROM processed_tweets
            WHERE tweet_date >= date_sub(current_date(), 30)
            GROUP BY tweet_date, tweet_hour, sentiment_label
            ORDER BY tweet_date DESC, tweet_hour DESC
            """
            
            main_df = hive_ops.execute_query(sentiment_query)
            
            # Add calculated columns for Power BI
            main_df['engagement_score'] = main_df['total_likes'] + main_df['total_retweets']
            main_df['sentiment_strength'] = main_df['avg_polarity'].abs()
            main_df['datetime'] = pd.to_datetime(main_df['tweet_date'].astype(str) + ' ' + 
                                               main_df['tweet_hour'].astype(str) + ':00:00')
            
            # Export main dataset
            main_df.to_csv('data/dashboard/main_sentiment_data.csv', index=False)
            
            self.logger.info("Power BI dataset created successfully")
            return main_df
            
        except Exception as e:
            self.logger.error(f"Error creating Power BI dataset: {e}")
            return pd.DataFrame()