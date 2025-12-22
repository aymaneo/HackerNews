# Hacker News Data Pipeline with Spark NLP

Big Data project for analyzing Hacker News stories and comments using Apache Spark, Kafka, Delta Lake, and Spark NLP.

## Quick Start

### 1. Install Dependencies

```bash
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Start Kafka Infrastructure

```bash
docker-compose up -d
```

This starts:
- Zookeeper (port 2181)
- Kafka (port 9092)
- Kafka UI (http://localhost:8080)

### 3. Test Hacker News API Client

```bash
python hn_api_client.py
```

This will fetch the top 5 stories and display their details.

### 4. Run Kafka Producer

**One-time fetch (top stories):**
```bash
python kafka_producer.py --mode top
```

**One-time fetch (new stories):**
```bash
python kafka_producer.py --mode new
```

**Continuous mode:**
```bash
python kafka_producer.py --mode top --continuous
```

## Kafka Topics

The producer creates three topics:

- `hn-stories`: Story items from Hacker News
- `hn-comments`: Comments on stories
- `hn-users`: User profile data

## Monitor Kafka

### Using Kafka UI
Open http://localhost:8080 in your browser to see:
- Topics and their messages
- Consumer groups
- Broker information
