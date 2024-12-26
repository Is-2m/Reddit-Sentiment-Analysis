#!/bin/bash

# Start Hadoop services
$HADOOP_HOME/bin/hdfs namenode -format
$HADOOP_HOME/sbin/start-dfs.sh
$HADOOP_HOME/sbin/start-yarn.sh

# Start Reddit producer in the background
# cd /app/src/producer
# python3 reddit_producer.py &

# Start the Streamlit dashboard in the background
# cd /app/src/visualization
# streamlit run dashboard.py --server.port 8501  &


# Keep the container running
tail -f /dev/null