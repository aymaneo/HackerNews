#!/usr/bin/env python3
"""Kafka producer for Hacker News stories and comments."""

import json
import logging
import os
import time
import requests
from confluent_kafka import Producer

KAFKA_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
FETCH_INTERVAL = int(os.getenv('FETCH_INTERVAL', '60'))
HN_API = 'https://hacker-news.firebaseio.com/v0'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
)
logger = logging.getLogger('hn-producer')

producer = Producer({'bootstrap.servers': KAFKA_SERVERS})

while True:
    try:
        story_ids = requests.get(f'{HN_API}/topstories.json', timeout=10).json()[:30]

        for story_id in story_ids:
            try:
                story = requests.get(f'{HN_API}/item/{story_id}.json', timeout=10).json()
                if not story or story.get('type') != 'story':
                    continue

                producer.produce('hn-stories', key=str(story_id).encode(), value=json.dumps(story).encode())
                logger.info("Story produced: %s", story.get('title'))

                for comment_id in story.get('kids', [])[:50]:
                    try:
                        comment = requests.get(f'{HN_API}/item/{comment_id}.json', timeout=10).json()
                        if comment:
                            producer.produce('hn-comments', key=str(comment_id).encode(), value=json.dumps(comment).encode())
                    except requests.RequestException:
                        continue
            except requests.RequestException:
                continue

        producer.flush()
    except requests.RequestException as e:
        logger.warning("API error: %s", e)
    finally:
        logger.info("Waiting %ss...", FETCH_INTERVAL)
        time.sleep(FETCH_INTERVAL)
