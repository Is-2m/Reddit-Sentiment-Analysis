import streamlit as st
import happybase
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta
import time


class SentimentDashboard:
    def __init__(self):
        self.connection = happybase.Connection("localhost")
        self.table = self.connection.table("reddit_sentiments")

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

    def get_recent_sentiments(self, minutes=5):
        print("Fetching recent sentiments...")
        cutoff_time = datetime.now() - timedelta(minutes=minutes)

        data = []
        try:
            print("Scanning HBase table...")
            for key, value in self.table.scan():
                try:
                    text = value.get(b"post_data:text", b"").decode()
                    sentiment = value.get(b"sentiment:label", b"").decode()
                    score = float(value.get(b"sentiment:score", b"0").decode())

                    try:
                        timestamp = float(
                            value.get(b"post_data:timestamp", time.time()).decode()
                        )
                    except (ValueError, AttributeError):
                        timestamp = time.time()

                    post_time = datetime.fromtimestamp(timestamp)

                    if post_time > cutoff_time:
                        data.append(
                            {
                                "id": key.decode(),
                                "text": text,
                                "original_sentiment": sentiment,  # Keep original sentiment
                                "sentiment": self.convert_sentiment(
                                    sentiment
                                ),  # Add converted sentiment
                                "score": score,
                                "timestamp": post_time,
                            }
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
            st.experimental_rerun()

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
    while True:
        try:
            dashboard.render_dashboard()
            time.sleep(5)  # Refresh every 5 seconds
        except Exception as e:
            st.error(f"An error occurred: {str(e)}")
            # time.sleep(5)  # Wait before retrying
