# One image for every Spark container: the master, the workers (and the executors they
# start), the history server and the job submitters (drivers). A job can therefore only
# import what is installed here, and the driver and executors always run the same Python.
FROM apache/spark:3.5.8-scala2.12-java17-python3-ubuntu

USER root

# Cassandra connector, for df.write.format("org.apache.spark.sql.cassandra"). The assembly
# bundles the Cassandra Java driver (shaded). 3.5.x is the connector's last line and
# supports Spark 3.5 only, which is why the cluster is not on Spark 4.
ARG CASSANDRA_CONNECTOR_VERSION=3.5.1
RUN curl -fsSL -o /opt/spark/jars/spark-cassandra-connector-assembly_2.12-${CASSANDRA_CONNECTOR_VERSION}.jar \
      https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector-assembly_2.12/${CASSANDRA_CONNECTOR_VERSION}/spark-cassandra-connector-assembly_2.12-${CASSANDRA_CONNECTOR_VERSION}.jar

COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt && rm /tmp/requirements.txt

COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY jobs/ /opt/jobs/

USER spark
