FROM python:3.9-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Set working directory
WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create necessary directories
RUN mkdir -p data/raw data/processed data/dashboard logs

# Expose port
EXPOSE 8000

# Run application
CMD ["python", "main.py"]