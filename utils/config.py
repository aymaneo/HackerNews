"""
Configuration file for Hacker News pipeline
"""

# Hacker News API
HN_API_BASE_URL = "https://hacker-news.firebaseio.com/v0"
HN_API_ENDPOINTS = {
    "top_stories": f"{HN_API_BASE_URL}/topstories.json",
    "new_stories": f"{HN_API_BASE_URL}/newstories.json",
    "best_stories": f"{HN_API_BASE_URL}/beststories.json",
    "ask_stories": f"{HN_API_BASE_URL}/askstories.json",
    "show_stories": f"{HN_API_BASE_URL}/showstories.json",
    "job_stories": f"{HN_API_BASE_URL}/jobstories.json",
    "item": f"{HN_API_BASE_URL}/item/{{item_id}}.json",
    "user": f"{HN_API_BASE_URL}/user/{{user_id}}.json",
}

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = ['kafka:29092']
KAFKA_TOPICS = {
    "stories": "hn-stories",
    "comments": "hn-comments",
    "users": "hn-users",
}

# Producer settings
FETCH_INTERVAL_SECONDS = 60  # Fetch new stories every 60 seconds
MAX_STORIES_PER_BATCH = 30   # Fetch top 30 stories
MAX_COMMENTS_PER_STORY = 50  # Fetch up to 50 comments per story

# Rate limiting (to respect HN API)
REQUEST_DELAY_SECONDS = 0.1  # 100ms delay between requests
