#!/bin/bash

# Start Hadoop services
$HADOOP_HOME/sbin/hadoop-daemon.sh start datanode
$HADOOP_HOME/sbin/yarn-daemon.sh start nodemanager

# Start Spark consumer in the background
# cd /app/src/consumer
# python3 spark_consumer.py &

# Keep the container running
tail -f /dev/null