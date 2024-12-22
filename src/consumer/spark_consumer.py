                                                                from pyspark.sql import SparkSession
                                                                from pyspark.sql.functions import *
                                                                from pyspark.sql.types import *
                                                                from transformers import pipeline
                                                                import happybase


                                                                class SparkStreamProcessor:
                                                                    def __init__(self):
                                                                        self.spark = (
                                                                            SparkSession.builder.appName("RedditSentimentAnalysis")
                                                                            .config(
                                                                                "spark.jars.packages",
                                                                                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
                                                                            )
                                                                            .getOrCreate()
                                                                        )

                                                                        self.hbase_connection = happybase.Connection("localhost", port=9090)
                                                                        self.ensure_hbase_table()

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
                                                                        # Process each row individually
                                                                        for row in df.collect():
                                                                            try:
                                                                                sentiment_label, sentiment_score = self.analyze_sentiment(row.text)

                                                                                # Store in HBase
                                                                                table = self.hbase_connection.table("reddit_sentiments")
                                                                                table.put(
                                                                                    row.id.encode(),
                                                                                    {
                                                                                        b"post_data:text": row.text.encode(),
                                                                                        b"post_data:timestamp": str(
                                                                                            row.timestamp.timestamp()
                                                                                        ).encode(),  # Convert to Unix timestamp
                                                                                        b"sentiment:label": str(sentiment_label).encode(),
                                                                                        b"sentiment:score": str(sentiment_score).encode(),
                                                                                    },
                                                                                )
                                                                            except Exception as e:
                                                                                print(f"Error processing row {row.id}: {str(e)}")

                                                                    def start_streaming(self):
                                                                        # Define schema for the incoming data
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

                                                                        # Create streaming DataFrame from Kafka
                                                                        streaming_df = (
                                                                            self.spark.readStream.format("kafka")
                                                                            .option("kafka.bootstrap.servers", "localhost:9092")
                                                                            .option("subscribe", "reddit_posts")
                                                                            .load()
                                                                        )

                                                                        # Parse JSON data from Kafka
                                                                        parsed_df = streaming_df.select(
                                                                            from_json(col("value").cast("string"), schema).alias("data")
                                                                        ).select("data.*")

                                                                        # Start the streaming query
                                                                        query = parsed_df.writeStream.foreachBatch(self.process_batch).start()

                                                                        # Wait for the streaming to finish
                                                                        query.awaitTermination()


                                                                if __name__ == "__main__":
                                                                    # Initialize and start the processor
                                                                    processor = SparkStreamProcessor()
                                                                    processor.start_streaming()
