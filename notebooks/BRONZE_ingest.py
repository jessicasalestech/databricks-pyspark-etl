# Databricks notebook source
# MAGIC %md
# MAGIC # BRONZE — Raw Ingestion
# MAGIC
# MAGIC **Layer:** Bronze (landing)
# MAGIC **Owner:** Jessica Sales
# MAGIC
# MAGIC Loads the raw sample CSV from the `data/` directory into the bronze layer as
# MAGIC a Delta table with an audit/source column. In a real Databricks workspace the
# MAGIC source path would be a mounted volume, Autoloader (CloudFiles) or an external
# MAGIC landing zone. This notebook is the **first** step of the Medallion pipeline and
# MAGIC is meant to be run before `SILVER_transform`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

# Source of truth for the current notebook run
SOURCE_PATH = "/dbfs/FileStore/sample_data/raw_orders_sample.csv"
BRONZE_TABLE = "default.bronze_orders"
GOLD_CATALOG = "hive_metastore"

spark.conf.set("spark.sql.shuffle.partitions", "4")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read raw CSV with schema inference

# COMMAND ----------

from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# Read with an explicit header and a permissive mode so ragged rows do not
# abort the whole ingestion.
df_raw = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("mode", "PERMISSIVE")
    .load(SOURCE_PATH)
)

print(f"Loaded {df_raw.count()} raw rows from {SOURCE_PATH}")
display(df_raw.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Add audit columns and write the bronze Delta table

# COMMAND ----------

from pyspark.sql import functions as F

df_bronze = (
    df_raw
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.lit(SOURCE_PATH))
    .withColumn("_is_duplicate", F.col("order_id").isNull())
)

if spark.catalog.tableExists(f"{GOLD_CATALOG}.{BRONZE_TABLE}"):
    df_bronze.write.mode("overwrite").saveAsTable(f"{GOLD_CATALOG}.{BRONZE_TABLE}")
else:
    df_bronze.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        f"{GOLD_CATALOG}.{BRONZE_TABLE}"
    )

print(f"Wrote {df_bronze.count()} rows to bronze table {BRONZE_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC Now run **`SILVER_transform`** to cleanse and conform this table.