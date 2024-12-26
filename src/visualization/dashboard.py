import streamlit as st
import happybase
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta
import time


class SentimentDashboard:
    def __init__(self):
        try:
            self.connection = happybase.Connection("localhost", port=9090)
            print("Available tables:", self.connection.tables())  # Debug print

            if b"reddit_sentiments" not in self.connection.tables():
                raise Exception("reddit_sentiments table not found")

            self.table = self.connection.table("reddit_sentiments")
            # Try to get one row to verify connection
            for key, value in self.table.scan(limit=1):
                print("Successfully read one row from HBase")
                print(f"Sample key: {key}")
                break
        except Exception as e:
            print(f"Error initializing HBase connection: {e}")
            raise

    @staticmethod
    def convert_sentiment(stars):
        # Convert star rating to sentiment category
        try:
            stars = int(stars.split()[0])  # Extract number from "X STARS" format
            if stars <= 2:
                return "NEGATIVE"
            elif stars == 3:
                return "NEUTRAL"
            else:
                return "POSITIVE"
        except:
            return "NEUTRAL"

    def get_recent_sentiments(self, hours=24):  # Changed from minutes=5 to hours=24
        print("Fetching recent sentiments...")
        cutoff_time = datetime.now() - timedelta(
            hours=hours
        )  # Changed from minutes to hours
        print(f"Cutoff time: {cutoff_time}")

        data = []
        try:
            print("Scanning HBase table...")
            for key, value in self.table.scan():
                print(f"Processing key: {key}")
                try:
                    text = value.get(b"post_data:text", b"").decode()
                    sentiment = value.get(b"sentiment:label", b"").decode()
                    score = float(value.get(b"sentiment:score", b"0").decode())

                    try:
                        timestamp_bytes = value.get(
                            b"post_data:timestamp", str(time.time()).encode()
                        )
                        timestamp = float(timestamp_bytes.decode())
                        post_time = datetime.fromtimestamp(timestamp)
                        print(f"Timestamp: {timestamp}, Post time: {post_time}")
                    except (ValueError, AttributeError) as e:
                        print(f"Timestamp error: {e}")
                        timestamp = time.time()
                        post_time = datetime.fromtimestamp(timestamp)

                    if post_time > cutoff_time:
                        data.append(
                            {
                                "id": key.decode(),
                                "text": text,
                                "original_sentiment": sentiment,
                                "sentiment": self.convert_sentiment(sentiment),
                                "score": score,
                                "timestamp": post_time,
                            }
                        )
                    else:
                        print(
                            f"Skipping record: post_time {post_time} is before cutoff {cutoff_time}"
                        )
                except Exception as e:
                    print(f"Error processing record {key}: {str(e)}")
                    continue
        except Exception as e:
            print(f"Error scanning HBase: {str(e)}")
            raise

        print(f"Found {len(data)} records")
        return pd.DataFrame(data)

    def render_dashboard(self):
        st.title("Reddit Sentiment Analysis - Real-time Dashboard")

        # Add refresh button
        if st.button("Refresh Data"):
            st.rerun()  # Using rerun() instead of experimental_rerun()

        # Get recent data
        df = self.get_recent_sentiments()

        if df.empty:
            st.warning("No data available in the last 5 minutes. Try refreshing.")
            return

        # Display metrics
        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric("Total Posts Analyzed", len(df))

        with col2:
            positive_pct = (
                (df["sentiment"] == "POSITIVE").mean() * 100 if not df.empty else 0
            )
            st.metric("Positive Sentiment %", f"{positive_pct:.1f}%")

        with col3:
            negative_pct = (
                (df["sentiment"] == "NEGATIVE").mean() * 100 if not df.empty else 0
            )
            st.metric("Negative Sentiment %", f"{negative_pct:.1f}%")

        # Plot sentiment distribution
        fig = px.pie(df, names="sentiment", title="Sentiment Distribution")
        st.plotly_chart(fig)

        # Show recent posts with sentiments
        st.subheader("Recent Posts and Sentiments")

        # Create a more concise dataframe for display
        display_df = df.copy()
        display_df["text"] = display_df["text"].apply(
            lambda x: x[:100] + "..." if len(x) > 100 else x
        )

        # Add interactivity for text viewing
        for index, row in display_df.iterrows():
            with st.expander(f"Post {index + 1} - {row['sentiment']}"):
                st.write("**Full Text:**")
                st.write(df.iloc[index]["text"])
                st.write("**Sentiment Details:**")
                col1, col2 = st.columns(2)
                with col1:
                    st.write(f"Category: {row['sentiment']}")
                with col2:
                    st.write(f"Original Rating: {row['original_sentiment']}")
                st.write(f"Score: {row['score']:.2f}")
                st.write(f"Timestamp: {row['timestamp']}")


if __name__ == "__main__":
    dashboard = SentimentDashboard()

    # Instead of using a while loop, we let Streamlit handle the refresh
    try:
        dashboard.render_dashboard()
    except Exception as e:
        st.error(f"An error occurred: {str(e)}")
