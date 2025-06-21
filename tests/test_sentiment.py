import unittest
import pandas as pd
from scripts.processing.sentiment_analyzer import SentimentAnalyzer

class TestSentimentAnalyzer(unittest.TestCase):
    def setUp(self):
        self.analyzer = SentimentAnalyzer()
        
    def test_clean_text(self):
        """Test text cleaning functionality"""
        text = "Check out this link: https://example.com @user #hashtag"
        cleaned = self.analyzer.clean_text(text)
        self.assertNotIn("https://", cleaned)
        self.assertNotIn("@user", cleaned)
        self.assertNotIn("#hashtag", cleaned)
        
    def test_positive_sentiment(self):
        """Test positive sentiment detection"""
        text = "I love this amazing product! It's fantastic!"
        result = self.analyzer.analyze_sentiment(text)
        self.assertEqual(result['sentiment_label'], 'positive')
        self.assertGreater(result['polarity'], 0)
        
    def test_negative_sentiment(self):
        """Test negative sentiment detection"""
        text = "This is terrible! I hate it so much!"
        result = self.analyzer.analyze_sentiment(text)
        self.assertEqual(result['sentiment_label'], 'negative')
        self.assertLess(result['polarity'], 0)
        
    def test_neutral_sentiment(self):
        """Test neutral sentiment detection"""
        text = "This is a regular statement about something."
        result = self.analyzer.analyze_sentiment(text)
        self.assertEqual(result['sentiment_label'], 'neutral')
        self.assertAlmostEqual(result['polarity'], 0, delta=0.2)
        
    def test_batch_analyze(self):
        """Test batch sentiment analysis"""
        test_data = pd.DataFrame({
            'text': [
                'I love this!',
                'This is awful!',
                'This is okay.'
            ]
        })
        
        result_df = self.analyzer.batch_analyze(test_data)
        
        self.assertEqual(len(result_df), 3)
        self.assertIn('sentiment_label', result_df.columns)
        self.assertIn('polarity', result_df.columns)

if __name__ == '__main__':
    unittest.main()