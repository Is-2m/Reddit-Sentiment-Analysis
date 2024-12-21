# docker/hadoop-master/Dockerfile
FROM apache/hadoop:3

# Install Python and pip using dnf (for RHEL/CentOS based images)
RUN dnf update -y && \
    dnf install -y python3 python3-pip && \
    dnf clean all && \
    pip3 install pyspark pandas numpy scikit-learn

# Set working directory
WORKDIR /app

# Copy application files
COPY src/ /app/src/
COPY requirements.txt /app/

# Install Python dependencies
RUN pip3 install -r requirements.txt

# Expose Hadoop ports
EXPOSE 9870 8088 9864

# Set environment variables
ENV HADOOP_HOME=/opt/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

# Start Hadoop services
CMD ["start-all.sh"]