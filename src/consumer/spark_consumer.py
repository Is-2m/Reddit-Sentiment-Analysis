from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from transformers import pipeline
import happybase
import logging


class SparkStreamProcessor:

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.spark = (
            SparkSession.builder.appName("RedditSentimentAnalysis")
            .config(
                "spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
            )
            .config("spark.ui.port", "4040")
            .getOrCreate()
        )

        self.hbase_connection = happybase.Connection("localhost", port=9090)
        # self.hbase_connection = happybase.Connection("hbase", port=9090)
        # Add this line to check connection
        self.check_hbase_connection()
        self.ensure_hbase_table()

    def check_hbase_connection(self):
        try:
            tables = self.hbase_connection.tables()
            print(f"Connected to HBase. Available tables: {tables}")
            return True
        except Exception as e:
            print(f"HBase connection failed: {str(e)}")
            raise

    def ensure_hbase_table(self):
        if b"reddit_sentiments" not in self.hbase_connection.tables():
            self.hbase_connection.create_table(
                "reddit_sentiments", {"post_data": dict(), "sentiment": dict()}
            )

    @staticmethod
    def analyze_sentiment(text):
        # Initialize pipeline for each analysis to avoid serialization issues
        sentiment_analyzer = pipeline(
            "sentiment-analysis",
            model="nlptown/bert-base-multilingual-uncased-sentiment",
        )
        result = sentiment_analyzer(text)[0]
        return result["label"], result["score"]

    def process_batch(self, df, epoch_id):
        try:
            row_count = df.count()
            self.logger.info(f"Processing batch {epoch_id} with {row_count} rows")

            for row in df.collect():
                try:
                    self.logger.info(f"Processing post ID: {row.id}")

                    # Analyze sentiment
                    sentiment_label, sentiment_score = self.analyze_sentiment(row.text)
                    self.logger.info(
                        f"Sentiment analysis complete for {row.id}: {sentiment_label}"
                    )

                    # Store in HBase
                    table = self.hbase_connection.table("reddit_sentiments")
                    row_key = row.id.encode()
                    data = {
                        b"post_data:text": row.text.encode(),
                        b"post_data:timestamp": str(row.timestamp.timestamp()).encode(),
                        b"sentiment:label": str(sentiment_label).encode(),
                        b"sentiment:score": str(sentiment_score).encode(),
                    }

                    self.logger.info(f"Attempting to store in HBase: {row_key}")
                    table.put(row_key, data)
                    self.logger.info(f"Successfully stored in HBase: {row_key}")

                except Exception as e:
                    self.logger.error(
                        f"Error processing row {row.id}: {str(e)}", exc_info=True
                    )

        except Exception as e:
            self.logger.error(
                f"Error processing batch {epoch_id}: {str(e)}", exc_info=True
            )

    def start_streaming(self):
        try:  # Define schema for the incoming data
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

            self.logger.info("Creating streaming DataFrame from Kafka")
            # Create streaming DataFrame from Kafka
            streaming_df = (
                self.spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                # .option("kafka.bootstrap.servers", "kafka:9092")
                .option("subscribe", "reddit_posts")
                .option("startingOffsets", "earliest")
                .load()
            )

            # Parse JSON data from Kafka
            self.logger.info("Parsing JSON data from Kafka")
            parsed_df = streaming_df.select(
                from_json(col("value").cast("string"), schema).alias("data")
            ).select("data.*")

            self.logger.info("Starting streaming query")
            query = (
                parsed_df.writeStream.foreachBatch(self.process_batch)
                .option("checkpointLocation", "/tmp/checkpoint")
                .start()
            )

            self.logger.info("Streaming query started, waiting for termination")
            query.awaitTermination()

        except Exception as e:
            self.logger.error(f"Error in streaming: {str(e)}", exc_info=True)
            raise


if __name__ == "__main__":
    # Initialize and start the processor
    processor = SparkStreamProcessor()
    processor.start_streaming()
