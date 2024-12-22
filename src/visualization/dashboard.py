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

    def get_recent_sentiments(self, minutes=5):
        cutoff_time = datetime.now() - timedelta(minutes=minutes)

        data = []
        for key, value in self.table.scan():
            try:
                # Get text and sentiment data
                text = value.get(b"post_data:text", b"").decode()
                sentiment = value.get(b"sentiment:label", b"").decode()
                score = float(value.get(b"sentiment:score", b"0").decode())

                # Handle timestamp - with fallback to current time if missing
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
                            "sentiment": sentiment,
                            "score": score,
                            "timestamp": post_time,
                        }
                    )
            except Exception as e:
                print(f"Error processing record {key}: {str(e)}")
                continue

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
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("Total Posts Analyzed", len(df))

        with col2:

            positive_pct = (
                (df["sentiment"].isin(["4 stars", "5 stars"])).mean() * 100
                if not df.empty
                else 0
            )
            st.metric("Positive Sentiment %", f"{positive_pct:.1f}%")

        with col3:
            negative_pct = (
                (df["sentiment"] == "3 stars").mean() * 100 if not df.empty else 0
            )
            st.metric("Neutral Sentiment %", f"{negative_pct:.1f}%")

        with col4:
            negative_pct = (
                (df["sentiment"].isin(["1 stars", "2 stars"])).mean() * 100
                if not df.empty
                else 0
            )
            st.metric("Negative Sentiment %", f"{negative_pct:.1f}%")

        # Plot sentiment distribution
        fig = px.pie(df, names="sentiment", title="Sentiment Distribution")
        st.plotly_chart(fig)

        # Show recent posts with sentiments
        st.subheader("Recent Posts and Sentiments")
        st.dataframe(df[["text", "sentiment", "score", "timestamp"]])


if __name__ == "__main__":
    dashboard = SentimentDashboard()
    while True:
        try:
            dashboard.render_dashboard()
            time.sleep(1.5)  # Refresh every 5 seconds
        except Exception as e:
            st.error(f"An error occurred: {str(e)}")
            # time.sleep(5)  # Wait before retrying
