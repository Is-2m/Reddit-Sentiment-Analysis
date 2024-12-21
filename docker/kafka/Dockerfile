FROM openjdk:11-jre-slim

# Install required packages (keep apt-get here since it's Debian-based)
RUN apt-get update && \
    apt-get install -y wget tar bash && \
    apt-get clean

# Set Kafka version
ENV KAFKA_VERSION=3.4.0
ENV SCALA_VERSION=2.13

# Download and extract Kafka
RUN wget https://downloads.apache.org/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz && \
    tar -xzf kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz && \
    mv kafka_${SCALA_VERSION}-${KAFKA_VERSION} /kafka && \
    rm kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz

# Set working directory
WORKDIR /kafka

# Copy custom configuration if needed
COPY config/server.properties /kafka/config/

# Expose Kafka ports
EXPOSE 9092

# Set environment variables
ENV KAFKA_HOME=/kafka
ENV PATH=$PATH:$KAFKA_HOME/bin

# Start Kafka
CMD ["bin/kafka-server-start.sh", "config/server.properties"]