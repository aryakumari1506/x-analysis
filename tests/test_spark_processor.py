import unittest
from unittest.mock import Mock, patch
from scripts.processing.spark_processor import SparkProcessor

class TestSparkProcessor(unittest.TestCase):
    def setUp(self):
        self.config = {
            'spark': {
                'app_name': 'TestSentimentAnalysis',
                'master': 'local[*]',
                'executor_memory': '1g',
                'driver_memory': '1g'
            }
        }
        
    @patch('pyspark.sql.SparkSession')
    def test_spark_initialization(self, mock_spark):
        """Test Spark session initialization"""
        processor = SparkProcessor(self.config)
        mock_spark.builder.appName.assert_called_with('TestSentimentAnalysis')
        
    def test_config_loading(self):
        """Test configuration loading"""
        processor = SparkProcessor(self.config)
        self.assertEqual(processor.config['spark']['app_name'], 'TestSentimentAnalysis')

if __name__ == '__main__':
    unittest.main()