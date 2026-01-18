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


def delivery_callback(err, msg):
    """Callback function to track message delivery success/failure."""
    if err:
        topic = msg.topic() if msg else 'unknown'
        logger.error("Message delivery failed: %s (topic=%s)", err, topic)
    else:
        logger.debug("Message delivered: topic=%s, partition=%s, offset=%s",
                     msg.topic(), msg.partition(), msg.offset())


producer = Producer({
    'bootstrap.servers': KAFKA_SERVERS,
    'acks': 'all',  # Wait for all in-sync replicas to acknowledge
    'retries': 3,  # Retry up to 3 times on transient errors
    'max.in.flight.requests.per.connection': 5,  # Max unacknowledged requests
    'compression.type': 'snappy',  # Enable compression for better throughput
})

while True:
    try:
        story_ids = requests.get(f'{HN_API}/topstories.json', timeout=10).json()[:30]

        for story_id in story_ids:
            try:
                story = requests.get(f'{HN_API}/item/{story_id}.json', timeout=10).json()
                if not story or story.get('type') != 'story':
                    continue

                producer.produce('hn-stories', key=str(story_id).encode(), 
                                value=json.dumps(story).encode(), callback=delivery_callback)
                logger.info("Story produced: %s", story.get('title'))

                for comment_id in story.get('kids', [])[:50]:
                    try:
                        comment = requests.get(f'{HN_API}/item/{comment_id}.json', timeout=10).json()
                        if comment:
                            producer.produce('hn-comments', key=str(comment_id).encode(), 
                                            value=json.dumps(comment).encode(), callback=delivery_callback)
                    except requests.RequestException as e:
                        logger.debug("Failed to fetch comment %s: %s", comment_id, e)
                        continue
            except requests.RequestException as e:
                logger.debug("Failed to fetch story %s: %s", story_id, e)
                continue

        producer.flush()
    except requests.RequestException as e:
        logger.warning("API error: %s", e)

    logger.info("Waiting %ss...", FETCH_INTERVAL)
    time.sleep(FETCH_INTERVAL)
