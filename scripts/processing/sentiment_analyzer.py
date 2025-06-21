from textblob import TextBlob
import re
import pandas as pd
from typing import Dict, List
import logging

class SentimentAnalyzer:
    def __init__(self):
        self.setup_logging()
        
    def setup_logging(self):
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
    def clean_text(self, text: str) -> str:
        """Clean tweet text for sentiment analysis"""
        # Remove URLs
        text = re.sub(r'http\S+|www\S+|https\S+', '', text, flags=re.MULTILINE)
        # Remove mentions and hashtags
        text = re.sub(r'@\w+|#\w+', '', text)
        # Remove extra whitespace
        text = ' '.join(text.split())
        return text.strip()
        
    def analyze_sentiment(self, text: str) -> Dict:
        """Analyze sentiment of text using TextBlob"""
        try:
            cleaned_text = self.clean_text(text)
            blob = TextBlob(cleaned_text)
            
            polarity = blob.sentiment.polarity
            subjectivity = blob.sentiment.subjectivity
            
            # Classify sentiment
            if polarity > 0.1:
                sentiment_label = 'positive'
            elif polarity < -0.1:
                sentiment_label = 'negative'
            else:
                sentiment_label = 'neutral'
                
            return {
                'cleaned_text': cleaned_text,
                'polarity': polarity,
                'subjectivity': subjectivity,
                'sentiment_label': sentiment_label,
                'confidence': abs(polarity)
            }
            
        except Exception as e:
            self.logger.error(f"Error analyzing sentiment: {e}")
            return {
                'cleaned_text': text,
                'polarity': 0.0,
                'subjectivity': 0.0,
                'sentiment_label': 'neutral',
                'confidence': 0.0
            }
            
    def batch_analyze(self, tweets_df: pd.DataFrame) -> pd.DataFrame:
        """Analyze sentiment for batch of tweets"""
        sentiment_results = []
        
        for _, tweet in tweets_df.iterrows():
            sentiment = self.analyze_sentiment(tweet['text'])
            sentiment_results.append(sentiment)
            
        # Add sentiment columns to dataframe
        sentiment_df = pd.DataFrame(sentiment_results)
        result_df = pd.concat([tweets_df.reset_index(drop=True), sentiment_df], axis=1)
        
        self.logger.info(f"Analyzed sentiment for {len(result_df)} tweets")
        return result_df