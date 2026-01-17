# Hacker News' API data processing pipeline

## Quick Start
### First time Setup
First, run garage once from the docker compose to set up the access keys
```bash
docker compose up -d garage-webui
```
Using garage UI available at **http://localhost:3909/** :
1. Set up an access key 
2. create a bucket named "bronze" accessed by that same access key. 
3. then update the following environment of kafka-connect-setup in the docker compose:  
   1. AWS_ACCESS_KEY_ID 
   2. AWS_SECRET_ACCESS_KEY

With this setup, garage will keep the access keys in its dedicated metadata folder.

### Launch the project
From the root run the following command :
```bash
docker-compose up 
```


---
## Available UIs
### Garage UI
Open in browser: **http://localhost:3909/**

### Spark UI
Open in browser: **http://localhost:8080/ui**

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
HN API → Kafka Producer → Kafka 
                             ↓
                    ┌────────────────┐
                    │  BRONZE Layer  │
                    │  (Raw Data)    │---------|
                    └────────────────┘         |   ← Spark + Delta Lake
                             ↓                 |      • HTML cleaning
                    ┌────────────────┐         |      • aggregation
                    │  SILVER Layer  │←--------|  
                    │  (Clean Data)  │            
                    └────────────────┘            
```

---


## Data Schema

### Bronze Layer (Raw from Kafka)
**Stories**: id, by, title, url, score, descendants, time, type, text, kids, _kafka_offset, _kafka_partition, _bronze_ingested_at

**Comments**: id, by, parent, story_id, text, time, type, kids, deleted, dead, _kafka_offset, _kafka_partition, _bronze_ingested_at

### Silver Layer (Cleaned)
**Stories**: id, author, title, url, score, comment_count, timestamp, text_raw, text_clean, has_url, has_text, type

**Comments**: id, author, story_id, parent, timestamp, text_raw, text_clean, has_text, word_count, char_count, has_replies, is_deleted, is_dead, quality_score, type

