#!/bin/bash

# Hadoop Setup Script
echo "Setting up Hadoop environment..."

# Create necessary directories
sudo mkdir -p /opt/hadoop/logs
sudo mkdir -p /tmp/hadoop-root/dfs/name
sudo mkdir -p /tmp/hadoop-root/dfs/data

# Set permissions
sudo chown -R $USER:$USER /opt/hadoop
sudo chown -R $USER:$USER /tmp/hadoop-root

# Format namenode (only on first setup)
if [ ! -d "/tmp/hadoop-root/dfs/name/current" ]; then
    echo "Formatting namenode..."
    hdfs namenode -format -force
fi

# Start Hadoop services
echo "Starting Hadoop services..."
start-dfs.sh
start-yarn.sh

# Create HDFS directories for sentiment analysis
echo "Creating HDFS directories..."
hdfs dfs -mkdir -p /sentiment_data
hdfs dfs -mkdir -p /sentiment_data/raw_tweets
hdfs dfs -mkdir -p /sentiment_data/processed_tweets
hdfs dfs -mkdir -p /sentiment_data/hourly_sentiment
hdfs dfs -mkdir -p /sentiment_data/daily_sentiment

# Set permissions
hdfs dfs -chmod 755 /sentiment_data
hdfs dfs -chmod 755 /sentiment_data/raw_tweets
hdfs dfs -chmod 755 /sentiment_data/processed_tweets
hdfs dfs -chmod 755 /sentiment_data/hourly_sentiment
hdfs dfs -chmod 755 /sentiment_data/daily_sentiment

echo "Hadoop setup completed!"

# Verify setup
echo "Verifying Hadoop installation..."
hdfs dfs -ls /
jps

echo "Setup verification completed!"