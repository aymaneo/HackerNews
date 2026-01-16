"""
Kafka Producer for Hacker News data
Fetches data from HN API and sends it to Kafka topics
"""

import json
import time
import logging
from datetime import datetime, timezone
from typing import Dict
import sys
import os
from confluent_kafka import Producer

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from hn_api_client import HackerNewsAPI
from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPICS,
    FETCH_INTERVAL_SECONDS,
    MAX_STORIES_PER_BATCH,
    MAX_COMMENTS_PER_STORY
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class HackerNewsProducer:
    """Kafka producer for Hacker News data"""

    def __init__(self):
        """Initialize Kafka producer and HN API client"""
        self.producer = self._create_producer()
        self.api = HackerNewsAPI()
        self.processed_stories = set()  # Track already processed stories
        self.message_count = 0

    def _create_producer(self) -> Producer:
        """
        Create and configure Kafka producer

        Returns:
            Configured Producer instance
        """
        try:
            config = {
                'bootstrap.servers': ','.join(KAFKA_BOOTSTRAP_SERVERS),
                'client.id': 'hackernews-producer',
                'acks': 'all',
                'retries': 3,
                'max.in.flight.requests.per.connection': 1
            }
            producer = Producer(config)
            logger.info(f"Kafka producer connected to {KAFKA_BOOTSTRAP_SERVERS}")
            return producer
        except Exception as e:
            logger.error(f"Failed to create Kafka producer: {e}")
            raise

    def delivery_report(self, err, msg):
        """
        Callback for message delivery reports

        Args:
            err: Error if delivery failed
            msg: Message that was delivered
        """
        if err is not None:
            logger.error(f'Message delivery failed: {err}')
        else:
            logger.debug(
                f'Message delivered to {msg.topic()} '
                f'[partition {msg.partition()}] at offset {msg.offset()}'
            )
            self.message_count += 1

    def send_to_kafka(self, topic: str, key: str, value: Dict) -> bool:
        """
        Send a message to Kafka topic

        Args:
            topic: Kafka topic name
            key: Message key
            value: Message value (dict)

        Returns:
            True if successful, False otherwise
        """
        try:
            # Add metadata
            value['_kafka_timestamp'] = datetime.now(timezone.utc).isoformat()

            # Serialize to JSON
            value_bytes = json.dumps(value).encode('utf-8')
            key_bytes = str(key).encode('utf-8') if key else None

            # Send message (asynchronous)
            self.producer.produce(
                topic=topic,
                key=key_bytes,
                value=value_bytes,
                callback=self.delivery_report
            )

            # Trigger delivery reports
            self.producer.poll(0)

            return True

        except Exception as e:
            logger.error(f"Failed to send message to {topic}: {e}")
            return False

    def process_story(self, story_id: int) -> bool:
        """
        Process a single story and its comments

        Args:
            story_id: The story ID

        Returns:
            True if successful, False otherwise
        """
        # Skip if already processed
        if story_id in self.processed_stories:
            return False

        logger.info(f"Processing story {story_id}...")

        # Get story details
        story = self.api.get_item(story_id)
        if not story:
            logger.warning(f"Could not fetch story {story_id}")
            return False

        # Only process stories (not jobs, polls, etc.)
        if story.get('type') != 'story':
            logger.info(f"Item {story_id} is not a story, skipping")
            return False

        # Send story to Kafka
        success = self.send_to_kafka(
            topic=KAFKA_TOPICS['stories'],
            key=str(story_id),
            value=story
        )

        if not success:
            return False

        # Process comments if they exist
        if 'kids' in story and story['kids']:
            comment_ids = story['kids'][:MAX_COMMENTS_PER_STORY]
            logger.info(f"Processing {len(comment_ids)} comments for story {story_id}")

            for comment_id in comment_ids:
                self.process_comment(comment_id, story_id)

        # Mark as processed
        self.processed_stories.add(story_id)
        logger.info(f"Successfully processed story {story_id}: {story.get('title', 'No title')}")

        return True

    def process_comment(self, comment_id: int, parent_story_id: int) -> bool:
        """
        Process a single comment

        Args:
            comment_id: The comment ID
            parent_story_id: The story this comment belongs to

        Returns:
            True if successful, False otherwise
        """
        comment = self.api.get_item(comment_id)
        if not comment:
            return False

        # Add story reference to comment
        comment['story_id'] = parent_story_id

        # Send comment to Kafka
        success = self.send_to_kafka(
            topic=KAFKA_TOPICS['comments'],
            key=str(comment_id),
            value=comment
        )

        return success

    def process_user(self, user_id: str) -> bool:
        """
        Process a user profile

        Args:
            user_id: The username

        Returns:
            True if successful, False otherwise
        """
        user = self.api.get_user(user_id)
        if not user:
            return False

        # Send user to Kafka
        success = self.send_to_kafka(
            topic=KAFKA_TOPICS['users'],
            key=user_id,
            value=user
        )

        return success

    def fetch_top_stories_batch(self) -> int:
        """
        Fetch and process a batch of top stories

        Returns:
            Number of stories processed
        """
        logger.info("Fetching top stories batch...")
        story_ids = self.api.get_top_stories(limit=MAX_STORIES_PER_BATCH)

        processed_count = 0
        for story_id in story_ids:
            if self.process_story(story_id):
                processed_count += 1

        # Wait for all messages to be delivered
        self.producer.flush()

        logger.info(f"Processed {processed_count} new stories")
        logger.info(f"Total messages sent: {self.message_count}")
        return processed_count

    def fetch_new_stories_batch(self) -> int:
        """
        Fetch and process a batch of new stories

        Returns:
            Number of stories processed
        """
        logger.info("Fetching new stories batch...")
        story_ids = self.api.get_new_stories(limit=MAX_STORIES_PER_BATCH)

        processed_count = 0
        for story_id in story_ids:
            if self.process_story(story_id):
                processed_count += 1

        # Wait for all messages to be delivered
        self.producer.flush()

        logger.info(f"Processed {processed_count} new stories")
        logger.info(f"Total messages sent: {self.message_count}")
        return processed_count

    def run_continuous(self, mode: str = 'top'):
        """
        Run producer in continuous mode

        Args:
            mode: 'top' for top stories, 'new' for new stories
        """
        logger.info(f"Starting continuous producer in '{mode}' mode...")
        logger.info(f"Fetch interval: {FETCH_INTERVAL_SECONDS} seconds")

        try:
            while True:
                try:
                    if mode == 'top':
                        self.fetch_top_stories_batch()
                    elif mode == 'new':
                        self.fetch_new_stories_batch()
                    else:
                        logger.error(f"Unknown mode: {mode}")
                        break

                    logger.info(f"Waiting {FETCH_INTERVAL_SECONDS} seconds before next fetch...")
                    time.sleep(FETCH_INTERVAL_SECONDS)

                except KeyboardInterrupt:
                    logger.info("Received interrupt signal, shutting down...")
                    break
                except Exception as e:
                    logger.error(f"Error in continuous mode: {e}")
                    time.sleep(10)  # Wait before retrying

        finally:
            self.close()

    def run_once(self, mode: str = 'top') -> int:
        """
        Run producer once and exit

        Args:
            mode: 'top' for top stories, 'new' for new stories

        Returns:
            Number of stories processed
        """
        logger.info(f"Running producer once in '{mode}' mode...")

        try:
            if mode == 'top':
                count = self.fetch_top_stories_batch()
            elif mode == 'new':
                count = self.fetch_new_stories_batch()
            else:
                logger.error(f"Unknown mode: {mode}")
                return 0

            return count

        finally:
            self.close()

    def close(self):
        """Close Kafka producer connection"""
        logger.info("Closing Kafka producer...")
        # Wait for all messages to be delivered
        remaining = self.producer.flush(timeout=10)
        if remaining > 0:
            logger.warning(f"{remaining} messages were not delivered")
        logger.info(f"Kafka producer closed. Total messages sent: {self.message_count}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Hacker News Kafka Producer')
    parser.add_argument(
        '--mode',
        choices=['top', 'new'],
        default='top',
        help='Story mode: top or new stories'
    )
    parser.add_argument(
        '--continuous',
        action='store_true',
        help='Run continuously'
    )

    args = parser.parse_args()

    producer = HackerNewsProducer()

    if args.continuous:
        producer.run_continuous(mode=args.mode)
    else:
        count = producer.run_once(mode=args.mode)
        print(f"\nProcessed {count} stories")
