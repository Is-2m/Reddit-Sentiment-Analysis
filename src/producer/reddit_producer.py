import praw
import json
from kafka import KafkaProducer
import logging
from datetime import datetime
import time


class RedditStreamProducer:
    def __init__(
        self,
        client_id,
        client_secret,
        password,
        user_agent,
        username,
        kafka_bootstrap_servers,
    ):
        self.reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            password=password,
            user_agent=user_agent,
            username=username,
        )

        self.producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            api_version=(2, 5, 0),
            security_protocol="PLAINTEXT",
        )

        self.setup_logging()

    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
        )
        self.logger = logging.getLogger(__name__)

    def stream_subreddit(self, subreddit_name, kafka_topic):
        subreddit = self.reddit.subreddit(subreddit_name)

        self.logger.info(f"Starting to stream from r/{subreddit_name}")

        for post in subreddit.stream.submissions():
            post_data = {
                "id": post.id,
                "title": post.title,
                "text": post.selftext,
                "score": post.score,
                "created_utc": post.created_utc,
                "num_comments": post.num_comments,
                "subreddit": subreddit_name,
                "timestamp": datetime.now().isoformat(),
            }

            self.producer.send(kafka_topic, post_data)
            self.logger.info(f"Sent post {post.id} to Kafka")


CLIENT_ID = "dMceiJplOJXQJTZLgU1EKg"
CLIENT_SECRET = "7XGpOgv6A4wpMIMdjH0XZtOsetm17g"
USERNAME = "PastAcrobatic524"
PASSWORD = "-dgPZs]34;-PdX~"
USER_AGENT = "script:sentiment-analysis:v0.1 (by /u/PastAcrobatic524)"

if __name__ == "__main__":
    producer = RedditStreamProducer(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        password=PASSWORD,
        user_agent=USER_AGENT,
        username=USERNAME,
        # kafka_bootstrap_servers=["kafka:9092"],
        kafka_bootstrap_servers=["localhost:9092"],
    )

    producer.stream_subreddit("mademesmile+suicidewatch+happy+linux", "reddit_posts")
