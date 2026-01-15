# Hacker News 

## Quick Start

### Option 1: One Command (Easiest!)
```bash
./run_pipeline.sh
```
This will install dependencies, start Kafka, fetch data, and run both Bronze and Silver layers.

### Option 2: Step by Step
```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Start Kafka
docker-compose up -d
sleep 15

# 3. Fetch data from HN API → Kafka
python3 utils/kafka_producer.py --mode top

# 4. Load Kafka → Bronze Delta Lake
python3 run_spark_bronze.py

# 5. Clean Bronze → Silver Delta Lake
python3 run_spark_silver.py
```


---



### Kafka UI
Open in browser: **http://localhost:8082**
- View topics: `hn-stories`, `hn-comments`

### Query Delta Lake (Jupyter Notebook)
```bash
jupyter notebook explore_data.ipynb
```

---

## 🏗️ Architecture

```
HN API → Kafka Producer → Kafka Topics
                             ↓
                    ┌────────────────┐
                    │  BRONZE Layer  │  ← Spark + Delta Lake
                    │  (Raw Data)    │     • Kafka → Delta
                    └────────────────┘     • ACID writes
                             ↓
                    ┌────────────────┐
                    │  SILVER Layer  │  ← Spark + Delta Lake
                    │  (Clean Data)  │     • HTML cleaning
                    └────────────────┘     • Quality scoring
```

---

## 📁 Project Structure

```
hackernews/
├── config/
│   └── config.py              # Configuration
├── utils/
│   ├── hn_api_client.py       # HN API client
│   └── kafka_producer.py      # Producer (API → Kafka)
├── bronze/
│   └── spark_loader.py        # Bronze (Kafka → Delta) 
├── silver/
│   └── spark_processor.py     # Silver (Bronze → Silver) 
├── data/
│   ├── bronze/                # Bronze Delta tables
│   │   ├── stories/
│   │   └── comments/
│   └── silver/                # Silver Delta tables
│       ├── stories/
│       └── comments/
├── docker-compose.yml         # Kafka infrastructure
├── requirements.txt
├── run_pipeline.sh            # One-command launcher
├── run_spark_bronze.py        # Run Bronze layer
├── run_spark_silver.py        # Run Silver layer
├── test_delta.py              # Test Delta Lake
└── explore_data.ipynb         # Query data (Jupyter)
```

---

## Data Schema

### Bronze Layer (Raw from Kafka)
**Stories**: id, by, title, url, score, descendants, time, type, text, kids, _kafka_offset, _kafka_partition, _bronze_ingested_at

**Comments**: id, by, parent, story_id, text, time, type, kids, deleted, dead, _kafka_offset, _kafka_partition, _bronze_ingested_at

### Silver Layer (Cleaned)
**Stories**: id, author, title, url, score, comment_count, timestamp, text_raw, text_clean, has_url, has_text, type

**Comments**: id, author, story_id, parent, timestamp, text_raw, text_clean, has_text, word_count, char_count, has_replies, is_deleted, is_dead, quality_score, type

