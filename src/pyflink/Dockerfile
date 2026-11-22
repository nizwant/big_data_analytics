# Hadoop client jars (hdfs:// support for Flink's filesystem connector and checkpoints).
FROM --platform=linux/amd64 apache/hadoop:3.4.2 AS hadoop

FROM flink:1.20.3-java17

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-pip python3-dev build-essential curl \
    && ln -s /usr/bin/python3 /usr/bin/python \
    && rm -rf /var/lib/apt/lists/*

# JAVA_HOME is required to build pemja, a dependency of apache-flink.
ENV JAVA_HOME=/opt/java/openjdk
RUN pip install --no-cache-dir apache-flink==1.20.3

# Kafka source, Avro with the schema registry, and Parquet format for the Table API.
RUN curl -fsSL -o /opt/flink/lib/flink-sql-connector-kafka-3.4.0-1.20.jar \
      https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar \
    && curl -fsSL -o /opt/flink/lib/flink-sql-avro-confluent-registry-1.20.3.jar \
      https://repo1.maven.org/maven2/org/apache/flink/flink-sql-avro-confluent-registry/1.20.3/flink-sql-avro-confluent-registry-1.20.3.jar \
    && curl -fsSL -o /opt/flink/lib/flink-sql-parquet-1.20.3.jar \
      https://repo1.maven.org/maven2/org/apache/flink/flink-sql-parquet/1.20.3/flink-sql-parquet-1.20.3.jar

COPY --from=hadoop /opt/hadoop/share/hadoop/common /opt/hadoop/share/hadoop/common
COPY --from=hadoop /opt/hadoop/share/hadoop/hdfs /opt/hadoop/share/hadoop/hdfs
# Parquet's writer needs FileOutputFormat from mapreduce-client-core.
COPY --from=hadoop /opt/hadoop/share/hadoop/mapreduce /opt/hadoop/share/hadoop/mapreduce
# Drop Hadoop's log4j 1.x binding so it does not clash with Flink's log4j2.
RUN find /opt/hadoop -name 'slf4j-reload4j*' -o -name 'reload4j*' | xargs rm -f
ENV HADOOP_CLASSPATH=/opt/hadoop/share/hadoop/common/*:/opt/hadoop/share/hadoop/common/lib/*:/opt/hadoop/share/hadoop/hdfs/*:/opt/hadoop/share/hadoop/hdfs/lib/*:/opt/hadoop/share/hadoop/mapreduce/*

COPY cran_logs_ingest.py cran_package_downloads.py /opt/jobs/

USER flink
