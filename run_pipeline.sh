#!/bin/bash
#
# Hacker News Data Pipeline - Easy Launcher
# IMT Atlantique - Data Large Scale Project
#

set -e  # Exit on error

echo "=================================================="
echo "  Hacker News Data Pipeline"
echo "  Spark + Delta Lake + Kafka"
echo "=================================================="
echo ""

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Step 1: Check Docker
echo -e "${BLUE}[1/4]${NC} Checking Docker..."
if ! docker ps &> /dev/null; then
    echo -e "${YELLOW}⚠️  Docker is not running. Please start Docker Desktop.${NC}"
    exit 1
fi
echo -e "${GREEN}✓${NC} Docker is running"
echo ""

# Step 2: Start Kafka
echo -e "${BLUE}[2/4]${NC} Starting Kafka infrastructure..."
docker-compose up -d
echo -e "${YELLOW}⏳ Waiting 15 seconds for Kafka to be ready...${NC}"
sleep 15
echo -e "${GREEN}✓${NC} Kafka is ready"
echo ""

# Step 3: Fetch data from HN API
echo -e "${BLUE}[3/4]${NC} Fetching data from Hacker News API..."
echo -e "  → Producing messages to Kafka topics..."
python3 utils/kafka_producer.py --mode top
echo -e "${GREEN}✓${NC} Data sent to Kafka"
echo ""

# Step 4: Run Spark pipeline
echo -e "${BLUE}[4/4]${NC} Running Spark + Delta Lake pipeline..."
echo ""

echo -e "  ${BLUE}[4a/4]${NC} Bronze Layer (Kafka → Delta Lake)..."
python3 run_spark_bronze.py
echo -e "${GREEN}✓${NC} Bronze layer complete"
echo ""

echo -e "  ${BLUE}[4b/4]${NC} Silver Layer (Cleaning & Quality Scoring)..."
python3 run_spark_silver.py
echo -e "${GREEN}✓${NC} Silver layer complete"
echo ""

# Success message
echo "=================================================="
echo -e "${GREEN}✅ PIPELINE COMPLETE!${NC}"
echo "=================================================="
echo ""
echo "Your data is now available in:"
echo "  • Bronze: data/bronze/stories, data/bronze/comments"
echo "  • Silver: data/silver/stories, data/silver/comments"
echo ""
echo "View Kafka UI: http://localhost:8080"
echo ""
echo "To query data, run:"
echo "  python3 -c 'from pyspark.sql import SparkSession; from delta import configure_spark_with_delta_pip; spark = configure_spark_with_delta_pip(SparkSession.builder).getOrCreate(); spark.read.format(\"delta\").load(\"data/silver/stories\").show()'"
echo ""
