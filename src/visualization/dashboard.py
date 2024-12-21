import streamlit as st
import happybase
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta
import time

class SentimentDashboard:
    def __init__(self):
        self.connection = happybase.Connection('localhost')
        self.table = self.connection.table('reddit_sentiments')

    def get_recent_sentiments(self, minutes=5):
        cutoff_time = datetime.now() - timedelta(minutes=minutes)
        
        data = []
        for key, value in self.table.scan():
            timestamp = float(value[b'post_data:timestamp'])
            if datetime.fromtimestamp(timestamp) > cutoff_time:
                data.append({
                    'id': key.decode(),
                    'text': value[b'post_data:text'].decode(),
                    'sentiment': value[b'sentiment:label'].decode(),
                    'score': float(value[b'sentiment:score'].decode()),
                    'timestamp': datetime.fromtimestamp(timestamp)
                })
        
        return pd.DataFrame(data)

    def render_dashboard(self):
        st.title("Reddit Sentiment Analysis - Real-time Dashboard")
        
        # Add refresh button
        if st.button("Refresh Data"):
            st.experimental_rerun()
        
        # Get recent data
        df = self.get_recent_sentiments()
        
        # Display metrics
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Total Posts Analyzed", len(df))
        
        with col2:
            positive_pct = (df['sentiment'] == 'POSITIVE').mean() * 100
            st.metric("Positive Sentiment %", f"{positive_pct:.1f}%")
        
        with col3:
            negative_pct = (df['sentiment'] == 'NEGATIVE').mean() * 100
            st.metric("Negative Sentiment %", f"{negative_pct:.1f}%")
        
        # Plot sentiment distribution
        fig = px.pie(df, names='sentiment', title='Sentiment Distribution')
        st.plotly_chart(fig)
        
        # Show recent posts with sentiments
        st.subheader("Recent Posts and Sentiments")
        st.dataframe(df[['text', 'sentiment', 'score', 'timestamp']])

if __name__ == "__main__":
    dashboard = SentimentDashboard()
    dashboard.render_dashboard()