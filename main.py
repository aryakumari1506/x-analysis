import os
import yaml
import schedule
import time
import logging
from datetime import datetime
from dotenv import load_dotenv

from scripts.data_ingestion.twitter_stream import TwitterStreamer
from scripts.processing.spark_processor import SparkProcessor
from scripts.storage.hive_operations import HiveOperations
from scripts.visualization.power_bi_connector import PowerBIConnector

# Load environment variables
load_dotenv()

def load_config():
    """Load configuration from YAML file"""
    with open('config/app-config.yaml', 'r') as file:
        config = yaml.safe_load(file)
    return config

def setup_logging():
    """Setup logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('sentiment_analysis.log'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

class SentimentAnalysisPipeline:
    def __init__(self):
        self.config = load_config()
        self.logger = setup_logging()
        self.twitter_streamer = TwitterStreamer(self.config)
        self.spark_processor = SparkProcessor(self.config)
        self.hive_ops = HiveOperations(self.config)
        
    def collect_tweets(self, keywords=['technology', 'ai', 'machine learning'], max_results=500):
        """Collect tweets for analysis"""
        self.logger.info("Starting tweet collection...")
        
        query = " OR ".join(keywords)
        tweets = self.twitter_streamer.search_tweets(query, max_results)
        
        if tweets:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"tweets_{timestamp}.json"
            self.twitter_streamer.save_tweets_to_file(tweets, filename)
            return filename
        return None
        
    def process_data(self, filename=None):
        """Process collected tweets"""
        self.logger.info("Starting data processing...")
        
        try:
            # Read raw data
            if filename:
                raw_df = self.spark_processor.read_json_files(f"data/raw/{filename}")
            else:
                raw_df = self.spark_processor.read_json_files("data/raw/*.json")
                
            if raw_df is None or raw_df.count() == 0:
                self.logger.warning("No data to process")
                return
                
            # Process tweets
            processed_df = self.spark_processor.process_tweets(raw_df)
            
            # Add sentiment analysis
            sentiment_df = self.spark_processor.add_sentiment_analysis(processed_df)
            
            # Save to HDFS
            hdfs_path = f"{self.config['hadoop']['hdfs_base_path']}/processed_tweets"
            self.spark_processor.save_to_hdfs(sentiment_df, hdfs_path)
            
            # Create aggregated views
            aggregated_views = self.spark_processor.create_aggregated_views(sentiment_df)
            
            if aggregated_views:
                # Save aggregated data
                for view_name, view_df in aggregated_views.items():
                    agg_path = f"{self.config['hadoop']['hdfs_base_path']}/{view_name}"
                    self.spark_processor.save_to_hdfs(view_df, agg_path, mode="overwrite")
                    
            self.logger.info("Data processing completed successfully")
            
        except Exception as e:
            self.logger.error(f"Error in data processing: {e}")
            
    def update_dashboard_data(self):
        """Update data for Power BI dashboard"""
        self.logger.info("Updating dashboard data...")
        
        try:
            # Get dashboard summary
            summary = self.hive_ops.get_dashboard_summary()
            
            # Get sentiment trends
            trends = self.hive_ops.get_sentiment_trends(days=7)
            
            # Export data for Power BI
            os.makedirs('data/dashboard', exist_ok=True)
            trends.to_csv('data/dashboard/sentiment_trends.csv', index=False)
            
            self.logger.info("Dashboard data updated successfully")
            
        except Exception as e:
            self.logger.error(f"Error updating dashboard data: {e}")
            
    def run_full_pipeline(self):
        """Run the complete pipeline"""
        self.logger.info("Starting full sentiment analysis pipeline...")
        
        try:
            # Step 1: Collect tweets
            filename = self.collect_tweets()
            
            # Step 2: Process data
            self.process_data(filename)
            
            # Step 3: Update dashboard data
            self.update_dashboard_data()
            
            self.logger.info("Pipeline completed successfully")
            
        except Exception as e:
            self.logger.error(f"Error in pipeline execution: {e}")
            
    def start_scheduler(self):
        """Start scheduled pipeline execution"""
        self.logger.info("Starting scheduler...")
        
        # Schedule pipeline to run every 5 minutes
        schedule.every(5).minutes.do(self.run_full_pipeline)
        
        # Schedule dashboard update every hour
        schedule.every().hour.do(self.update_dashboard_data)
        
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute

def main():
    """Main function"""
    pipeline = SentimentAnalysisPipeline()
    
    # Run once immediately
    pipeline.run_full_pipeline()
    
    # Start scheduled execution
    # pipeline.start_scheduler()

if __name__ == "__main__":
    main()