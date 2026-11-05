"""Local Spark session with an Iceberg Hadoop catalog named `local`."""
import os

from pyspark.sql import SparkSession

ICEBERG = "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0"


def get_spark(warehouse: str = "warehouse") -> SparkSession:
    return (
        SparkSession.builder.appName("iceberg-lineage-lab")
        .master("local[2]")
        # ICEBERG_JAR=/path/to.jar skips the Maven download (offline / proxied networks)
        .config(*(("spark.jars", os.environ["ICEBERG_JAR"]) if "ICEBERG_JAR" in os.environ
                  else ("spark.jars.packages", ICEBERG)))
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", warehouse)
        .config("spark.sql.defaultCatalog", "local")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
