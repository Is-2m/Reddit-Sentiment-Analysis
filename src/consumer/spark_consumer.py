from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from transformers import pipeline
import happybase


class SparkStreamProcessor:
    def __init__(self):
        self.spark = SparkSession.builder.appName(
            "RedditSentimentAnalysis"
        ).getOrCreate()

        self.sentiment_analyzer = pipeline(
            "sentiment-analysis",
            model="nlptown/bert-base-multilingual-uncased-sentiment",
        )

        self.hbase_connection = happybase.Connection("localhost")
        self.ensure_hbase_table()

    def ensure_hbase_table(self):
        if b"reddit_sentiments" not in self.hbase_connection.tables():
            self.hbase_connection.create_table(
                "reddit_sentiments", {"post_data": dict(), "sentiment": dict()}
            )

    def analyze_sentiment(self, text):
        result = self.sentiment_analyzer(text)[0]
        return result["label"], result["score"]

    def process_batch(self, df, epoch_id):
        # Convert DataFrame to RDD for parallel processing
        analyzed_rdd = df.rdd.map(
            lambda row: {
                "id": row.id,
                "text": row.text,
                "sentiment": self.analyze_sentiment(row.text),
            }
        )

        # Store in HBase
        table = self.hbase_connection.table("reddit_sentiments")

        for record in analyzed_rdd.collect():
            table.put(
                record["id"].encode(),
                {
                    b"post_data:text": record["text"].encode(),
                    b"sentiment:label": str(record["sentiment"][0]).encode(),
                    b"sentiment:score": str(record["sentiment"][1]).encode(),
                },
            )

    def start_streaming(self):
        schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("title", StringType(), True),
                StructField("text", StringType(), True),
                StructField("score", IntegerType(), True),
                StructField("created_utc", TimestampType(), True),
                StructField("num_comments", IntegerType(), True),
                StructField("subreddit", StringType(), True),
                StructField("timestamp", TimestampType(), True),
            ]
        )

        streaming_df = (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", "kafka:9092")
            .option("subscribe", "reddit_posts")
            .load()
        )

        parsed_df = streaming_df.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")

        query = parsed_df.writeStream.foreachBatch(self.process_batch).start()

        query.awaitTermination()


if __name__ == "__main__":
    processor = SparkStreamProcessor()
    processor.start_streaming()
