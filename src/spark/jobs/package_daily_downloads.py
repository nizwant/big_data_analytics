"""Daily downloads per package: the batch view batch_package_daily_downloads.

Reads the package downloads that Flink writes as Parquet to HDFS
/data/clean/cran_mirror_package_downloads/dt=YYYY-MM-DD/hr=HH (src/pyflink/cran_package_downloads.py)
and computes, per package and day (UTC, the dt partition):

- downloads: successful GET requests (outcome SUCCESS), the count a user would call downloads;
- failed_requests: requests of any method that failed (HTTP status 400 or above);
- unique_ips: distinct client addresses among the downloads;
- source_downloads, windows_downloads, macos_downloads: downloads per target system.

It writes the result to the Cassandra table cran_analytics.batch_package_daily_downloads
(created by deploy/cassandra_init.sh) and as Parquet to HDFS /data/batch/package_daily_downloads/dt=YYYY-MM-DD.

Every run recomputes all days, so it is idempotent: the Cassandra rows are overwritten
(an insert with the same key is an update) and the HDFS copy is replaced. A day that was
still filling up at the last run is corrected by the next one. Files that Flink has not
finalized yet (hidden, in progress until the next checkpoint) are skipped.

Run it with: docker compose --profile spark-jobs run --rm spark-package-daily-downloads
"""

import os
from collections import Counter

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

HDFS_ROOT = os.environ.get("HDFS_ROOT", "hdfs://namenode:8020/data")
SOURCE_PATH = f"{HDFS_ROOT}/clean/cran_mirror_package_downloads"
TARGET_PATH = f"{HDFS_ROOT}/batch/package_daily_downloads"
KEYSPACE = os.environ.get("CASSANDRA_KEYSPACE", "cran_analytics")
TABLE = "batch_package_daily_downloads"

# Only the columns the view needs, so Parquet reads only these. dt and hr are the
# partition directories, declared as strings so that Spark does not infer their types.
SOURCE_SCHEMA = StructType([
    StructField("ip", StringType()),
    StructField("http_method", StringType()),
    StructField("package_name", StringType()),
    StructField("target_system", StringType()),
    StructField("outcome", StringType()),
    StructField("dt", StringType()),
    StructField("hr", StringType()),
])


def count_if(condition):
    return F.count(F.when(condition, True))


def daily_downloads(spark):
    is_download = (F.col("outcome") == "SUCCESS") & (F.col("http_method") == "GET")
    return (
        spark.read.schema(SOURCE_SCHEMA).parquet(SOURCE_PATH)
        .groupBy("package_name", "dt")
        .agg(
            count_if(is_download).alias("downloads"),
            count_if(F.col("outcome") == "FAILURE").alias("failed_requests"),
            F.countDistinct(F.when(is_download, F.col("ip"))).alias("unique_ips"),
            *(
                count_if(is_download & (F.col("target_system") == system)).alias(f"{system}_downloads")
                for system in ("source", "windows", "macos")
            ),
        )
        .withColumn("day", F.to_date("dt"))
        .withColumn("computed_at", F.current_timestamp())
    )


def executor_hosts(spark):
    """Number of executors per host (the workers' hostnames), without the driver."""
    driver_host = spark.conf.get("spark.driver.host")
    infos = spark.sparkContext._jsc.sc().statusTracker().getExecutorInfos()
    return Counter(info.host() for info in infos if info.host() != driver_host)


def main():
    spark = SparkSession.builder.appName("package_daily_downloads").getOrCreate()
    # Through log4j rather than print, so the lines also reach the driver log in HDFS.
    log = spark._jvm.org.apache.logging.log4j.LogManager.getLogger("package_daily_downloads")

    daily = daily_downloads(spark).cache()

    # One file per day.
    (
        daily.drop("day")
        .repartition("dt")
        .write.mode("overwrite")
        .partitionBy("dt")
        .parquet(TARGET_PATH)
    )
    (
        daily.drop("dt")
        .write.format("org.apache.spark.sql.cassandra")
        .options(keyspace=KEYSPACE, table=TABLE)
        .mode("append")
        .save()
    )

    totals = daily.agg(F.count(F.lit(1)), F.countDistinct("dt"), F.sum("downloads")).first()
    hosts = ", ".join(f"{host} x{n}" for host, n in sorted(executor_hosts(spark).items()))
    log.info(
        f"Wrote {totals[0]} package-days ({totals[1]} days, {totals[2] or 0} downloads) "
        f"to {KEYSPACE}.{TABLE} and {TARGET_PATH}; executors: {hosts or 'none'}"
    )
    spark.stop()


if __name__ == "__main__":
    main()
