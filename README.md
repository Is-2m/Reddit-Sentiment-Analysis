# Real-time Reddit Sentiment Analysis with Big Data Technologies

This project implements a real-time sentiment analysis pipeline for Reddit posts, leveraging a suite of big data technologies including Hadoop, Kafka, Spark, HBase, and a Streamlit dashboard for visualization. The system is designed to ingest Reddit posts, analyze their sentiment, store the results, and display them in a live dashboard.

## 🚀 Project Overview

The core purpose of this project is to demonstrate a real-time data processing pipeline. It streams posts from Reddit, performs sentiment analysis using a pre-trained transformer model, stores the results in a NoSQL database, and visualizes the sentiment distribution and individual post sentiments on a web dashboard [1, 2].

## 🏗️ Architecture

The system is composed of several interconnected services orchestrated using Docker Compose [3]:

*   **Reddit Producer (`reddit_producer.py`)**: A Python application that uses the PRAW library to stream submissions from specified Reddit subreddits (e.g., `mademesmile` and `confessions`) [4, 5]. It publishes these posts as JSON messages to a Kafka topic named `reddit_posts` [5-7].
*   **Kafka**: A distributed streaming platform that acts as a message broker, ingesting Reddit posts from the producer [8, 9]. It is configured to create the `reddit_posts` topic automatically [10].
*   **Zookeeper**: Provides distributed coordination services for Kafka and HBase [9, 10].
*   **Spark Cluster (Master & Worker)**: A distributed processing engine [11, 12].
    *   **Spark Consumer (`spark_consumer.py`)**: A PySpark streaming application that consumes messages from the `reddit_posts` Kafka topic [7, 13].
    *   It parses the incoming JSON data based on a defined schema [14].
    *   For each batch of posts, it performs sentiment analysis [7, 15].
    *   The analyzed sentiment (label and score) along with the post data is then stored in HBase [16, 17].
*   **Hadoop HDFS (NameNode & DataNodes)**: Provides a distributed file system [18, 19]. Although not directly used for persistent data storage in the application logic (HBase is), the Spark cluster and HBase often rely on an underlying Hadoop environment. The Dockerfiles configure HDFS components [20, 21].
*   **Hadoop YARN (ResourceManager & NodeManagers)**: Manages cluster resources for distributed applications [22, 23]. Similar to HDFS, YARN is part of the foundational Hadoop setup [24, 25].
*   **HBase**: A NoSQL column-oriented database built on HDFS [10]. It is used to store the Reddit posts and their corresponding sentiment analysis results [26-28]. The table `reddit_sentiments` is automatically created if it doesn't exist, with column families for `post_data`, `sentiment`, and `metadata` [15, 29].
*   **Sentiment Analyzer (`analyzer.py`)**: A Python class that encapsulates the sentiment analysis logic using Hugging Face's `transformers` library [30]. It uses the `nlptown/bert-base-multilingual-uncased-sentiment` model [15, 30] and includes logic to truncate text to fit the model's token limit [31].
*   **Streamlit Dashboard (`dashboard.py`)**: A web application that connects to HBase using `happybase` [1] to fetch recent sentiment data [32, 33]. It visualizes the total posts analyzed, positive/negative sentiment percentages, and sentiment distribution via a pie chart [2, 34]. It also displays recent posts with their full text, sentiment details, and timestamps [35, 36].

## 🛠️ Technologies Used

*   **Docker** & **Docker Compose**: For containerization and orchestration of services [3].
*   **Apache Hadoop** (HDFS & YARN): Core big data framework for distributed storage and resource management [18, 19].
*   **Apache Kafka**: Distributed streaming platform [8].
*   **Apache Zookeeper**: Distributed coordination service [9].
*   **Apache Spark** (PySpark): For real-time stream processing and sentiment analysis [10, 13].
*   **Apache HBase**: NoSQL database for storing processed data [10, 26].
*   **Python**:
    *   `praw`: For interacting with the Reddit API [37].
    *   `kafka-python`: For Kafka producer functionality [37].
    *   `happybase`: Python client for HBase [1, 26].
    *   `transformers`: For sentiment analysis (utilizing pre-trained BERT model) [13, 30].
    *   `pyspark`: Python API for Spark [13].
    *   `streamlit`, `pandas`, `plotly.express`: For building the interactive dashboard [1].

## ⚙️ Setup and Installation

### Prerequisites

*   **Docker Desktop** or **Docker Engine** and **Docker Compose** installed on your system.

### Steps to Run

1.  **Clone the repository:**
    ```bash
    git clone <repository-url>
    cd <repository-directory>
    ```

2.  **Start the services using Docker Compose:**
    Navigate to the root directory of the project where `docker-compose.yml` is located.
    ```bash
    docker-compose up --build -d
    ```
    This command will:
    *   Build Docker images for `hadoop-master`, `hadoop-worker` (two instances: `hadoop-worker1`, `hadoop-worker2`), and Kafka (custom Dockerfile) [3, 8, 18, 19].
    *   Pull pre-built images for Zookeeper, HBase, and Spark (master and worker) [9-11].
    *   Create a `hadoop_network` bridge network for inter-service communication [12].
    *   Mount local volumes for Hadoop NameNode and DataNodes to persist data [3, 9, 38].
    *   Expose necessary ports:
        *   **Hadoop Master**: 9870 (NameNode UI), 8088 (ResourceManager UI) [3, 22]
        *   **Kafka**: 9092 (Kafka broker) [9, 39]
        *   **Zookeeper**: 2181 (Zookeeper client) [9]
        *   **HBase**: 16010 (HBase Master UI), 9090 (HBase client port), 16000 (HBase region server) [10]
        *   **Spark Master**: 8080 (Spark Master UI), 7077 (Spark RPC) [11]

3.  **Wait for services to initialize:**
    It might take a few minutes for all services (especially Hadoop and HBase) to start up completely and be ready. You can check the logs:
    ```bash
    docker-compose logs -f
    ```

## 🚀 Usage

Once all Docker containers are up and running:

1.  **Start the Reddit Producer:**
    This script will connect to Reddit and begin sending posts to Kafka.
    ```bash
    docker-compose exec hadoop-master python3 /app/reddit_producer.py
    ```
    *Note*: The `reddit_producer.py` uses specific Reddit API credentials embedded within the script for demonstration purposes [6].

2.  **Start the Spark Stream Processor:**
    This script will consume messages from Kafka, analyze sentiment, and store them in HBase.
    ```bash
    docker-compose exec spark-master /opt/bitnami/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 /app/spark_consumer.py
    ```

3.  **Access the Streamlit Dashboard:**
    Open your web browser and navigate to `http://localhost:8501`.
    *Note*: You will need to run the `dashboard.py` script locally outside of Docker for now, or build a Docker image for it. To run it locally, ensure you have Python and the required packages (`streamlit`, `happybase`, `pandas`, `plotly`) installed.

    ```bash
    # From your local machine, in the project root
    pip install -r requirements.txt # Ensure happybase and streamlit are installed
    streamlit run dashboard.py
    ```
    The dashboard will display real-time sentiment analysis results from HBase [1, 2]. You can click the "Refresh Data" button to update the displayed metrics and posts [2].

## 🛑 Stopping the Services

To stop and remove all Docker containers, networks, and volumes created by `docker-compose`:
```bash
docker-compose down -v

This command ensures a clean shutdown and removal of all associated resources.
