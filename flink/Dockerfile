# Custom Flink image: JARs and Python deps are baked in so they survive container restarts.
# Without this image, connector JARs and pip packages must be reinstalled on every new container.
FROM flink:1.18-scala_2.12-java11

USER root

# Install Python and pip
RUN apt-get update && \
    apt-get install -y python-is-python3 python3-pip wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Download Flink connector JARs
# Version 3.0.2-1.18 is the universal connector with all transitive dependencies shaded and bundled for Flink 1.18.
# Flink JDBC connector for database sinks (ClickHouse, Postgres, etc.)
# ClickHouse JDBC driver with all dependencies bundled (-all.jar)
# Prometheus reporter JAR required for metrics.reporter.prom config to work.
RUN cd /opt/flink/lib && \
    wget -q https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.2-1.18/flink-sql-connector-kafka-3.0.2-1.18.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.2-1.18/flink-connector-jdbc-3.1.2-1.18.jar && \
    wget -q https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.4.6/clickhouse-jdbc-0.4.6-all.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/flink/flink-metrics-prometheus/1.18.0/flink-metrics-prometheus-1.18.0.jar

# Install Python packages
RUN pip3 install --no-cache-dir \
    apache-flink==1.18.0 \
    clickhouse-driver \
    python-dotenv

# Create checkpoint directory with flink user ownership
RUN mkdir -p /tmp/flink-checkpoints && \
    chown -R flink:flink /tmp/flink-checkpoints

USER flink
