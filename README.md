# Hacker News 

## Quick Start

```bash
docker volume create garage-meta
docker volume create garage-data
docker-compose up -d
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


## Data Schema

### Bronze Layer (Raw from Kafka)
**Stories**: id, by, title, url, score, descendants, time, type, text, kids, _kafka_offset, _kafka_partition, _bronze_ingested_at

**Comments**: id, by, parent, story_id, text, time, type, kids, deleted, dead, _kafka_offset, _kafka_partition, _bronze_ingested_at

### Silver Layer (Cleaned)
**Stories**: id, author, title, url, score, comment_count, timestamp, text_raw, text_clean, has_url, has_text, type

**Comments**: id, author, story_id, parent, timestamp, text_raw, text_clean, has_text, word_count, char_count, has_replies, is_deleted, is_dead, quality_score, type

