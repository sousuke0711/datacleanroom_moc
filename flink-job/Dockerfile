FROM flink:1.17.2-scala_2.12-java11

# Install Python 3
RUN apt-get update -qq && \
    apt-get install -y --no-install-recommends python3 python3-pip wget && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

# Install PyFlink and job dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

# Download connector JARs into Flink's lib directory so they are auto-loaded
RUN wget -q -P /opt/flink/lib/ \
    "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-pulsar/4.0.0-1.17/flink-connector-pulsar-4.0.0-1.17.jar" && \
    wget -q -P /opt/flink/lib/ \
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.17/1.4.2/iceberg-flink-runtime-1.17-1.4.2.jar" && \
    wget -q -P /opt/flink/lib/ \
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" && \
    wget -q -P /opt/flink/lib/ \
    "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.540/aws-java-sdk-bundle-1.12.540.jar"

# Copy Python job files
COPY *.py /opt/flink/job/
COPY *.sql /opt/flink/job/

WORKDIR /opt/flink
