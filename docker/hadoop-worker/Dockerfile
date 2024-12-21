FROM ubuntu:22.04

# Avoid prompts from apt
ENV DEBIAN_FRONTEND=noninteractive

# Install OpenJDK and required dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-8-jdk \
    wget \
    python3 \
    python3-pip \
    python3-dev \
    curl \
    build-essential \
    gcc \
    apt-transport-https \
    ca-certificates \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Hadoop
ENV HADOOP_VERSION=3.3.6
RUN wget https://downloads.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz && \
    tar -xzf hadoop-${HADOOP_VERSION}.tar.gz && \
    mv hadoop-${HADOOP_VERSION} /opt/hadoop && \
    rm hadoop-${HADOOP_VERSION}.tar.gz

# Configure Hadoop
RUN mkdir -p /opt/hadoop/dfs/data

# Install Python packages
RUN pip3 install --no-cache-dir --upgrade pip && \
    pip3 install --no-cache-dir pyspark pandas numpy scikit-learn

# Set working directory
WORKDIR /app

# Copy application files
COPY src/ /app/src/
COPY requirements.txt /app/

# Install Python dependencies
RUN pip3 install --no-cache-dir -r requirements.txt

# Set environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV HADOOP_HOME=/opt/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV HDFS_DATANODE_USER=root
ENV YARN_NODEMANAGER_USER=root

# Expose DataNode and NodeManager ports
EXPOSE 9864 8042

# Start Hadoop DataNode service
COPY docker/hadoop-worker/start-datanode.sh /start-datanode.sh
RUN chmod +x /start-datanode.sh
CMD ["/start-datanode.sh"]