"""
Hacker News API Client
Fetches stories, comments, and user data from the HN API
"""
import logging
from typing import Optional, Dict, List

import requests
import time
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import HN_API_ENDPOINTS, REQUEST_DELAY_SECONDS

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class HackerNewsAPI:
    """Client for interacting with Hacker News API"""

    def __init__(self):
        self.base_url = HN_API_ENDPOINTS
        self.session = requests.Session()

    def _make_request(self, url: str) -> Optional[Dict]:
        """
        Make HTTP GET request with error handling

        Args:
            url: The API endpoint URL

        Returns:
            JSON response as dict or None if error
        """
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            time.sleep(REQUEST_DELAY_SECONDS)  # Rate limiting
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching {url}: {e}")
            return None

    def get_top_stories(self, limit: int = 30) -> List[int]:
        """
        Get IDs of top stories

        Args:
            limit: Maximum number of story IDs to return

        Returns:
            List of story IDs
        """
        logger.info(f"Fetching top {limit} stories...")
        data = self._make_request(self.base_url["top_stories"])
        if data:
            return data[:limit]
        return []

    def get_new_stories(self, limit: int = 30) -> List[int]:
        """
        Get IDs of new stories

        Args:
            limit: Maximum number of story IDs to return

        Returns:
            List of story IDs
        """
        logger.info(f"Fetching {limit} new stories...")
        data = self._make_request(self.base_url["new_stories"])
        if data:
            return data[:limit]
        return []

    def get_best_stories(self, limit: int = 30) -> List[int]:
        """
        Get IDs of best stories

        Args:
            limit: Maximum number of story IDs to return

        Returns:
            List of story IDs
        """
        logger.info(f"Fetching {limit} best stories...")
        data = self._make_request(self.base_url["best_stories"])
        if data:
            return data[:limit]
        return []

    def get_item(self, item_id: int) -> Optional[Dict]:
        """
        Get details of a specific item (story, comment, etc.)

        Args:
            item_id: The ID of the item

        Returns:
            Item data as dict or None
        """
        url = self.base_url["item"].format(item_id=item_id)
        return self._make_request(url)

    def get_user(self, user_id: str) -> Optional[Dict]:
        """
        Get user profile data

        Args:
            user_id: The username

        Returns:
            User data as dict or None
        """
        url = self.base_url["user"].format(user_id=user_id)
        return self._make_request(url)

    def get_story_with_comments(self, story_id: int, max_comments: int = 50) -> Dict:
        """
        Get a story along with its comments

        Args:
            story_id: The story ID
            max_comments: Maximum number of comments to fetch

        Returns:
            Dict containing story and comments
        """
        story = self.get_item(story_id)
        if not story:
            return {"story": None, "comments": []}

        comments = []
        if "kids" in story and story["kids"]:
            comment_ids = story["kids"][:max_comments]
            logger.info(f"Fetching {len(comment_ids)} comments for story {story_id}")

            for comment_id in comment_ids:
                comment = self.get_item(comment_id)
                if comment:
                    comments.append(comment)

        return {
            "story": story,
            "comments": comments
        }

    def get_all_comments_recursive(self, item_id: int, max_depth: int = 3) -> List[Dict]:
        """
        Recursively fetch all comments and their replies

        Args:
            item_id: The item ID to start from
            max_depth: Maximum depth of recursion

        Returns:
            List of all comments
        """
        if max_depth == 0:
            return []

        all_comments = []
        item = self.get_item(item_id)

        if item and item.get("type") in ["comment", "story"]:
            if item.get("type") == "comment":
                all_comments.append(item)

            if "kids" in item and item["kids"]:
                for kid_id in item["kids"]:
                    child_comments = self.get_all_comments_recursive(
                        kid_id,
                        max_depth - 1
                    )
                    all_comments.extend(child_comments)

        return all_comments


if __name__ == "__main__":
    # Test the API client
    api = HackerNewsAPI()

    print("Testing Hacker News API Client...\n")

    # Get top stories
    top_story_ids = api.get_top_stories(limit=5)
    print(f"Top 5 story IDs: {top_story_ids}\n")

    # Get details of first story
    if top_story_ids:
        story = api.get_item(top_story_ids[0])
        if story:
            print(f"First story details:")
            print(f"  Title: {story.get('title')}")
            print(f"  By: {story.get('by')}")
            print(f"  Score: {story.get('score')}")
            print(f"  Comments: {story.get('descendants', 0)}")
            print(f"  URL: {story.get('url', 'No URL')}\n")

            # Get some comments
            if story.get('kids'):
                comment_id = story['kids'][0]
                comment = api.get_item(comment_id)
                if comment:
                    print(f"First comment:")
                    print(f"  By: {comment.get('by')}")
                    text = comment.get('text', '')[:100]
                    print(f"  Text: {text}...")
