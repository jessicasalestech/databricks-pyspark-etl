# Databricks notebook source
# MAGIC %md
# MAGIC # SILVER — Clean & Conform
# MAGIC
# MAGIC **Layer:** Silver (conformed)
# MAGIC **Owner:** Jessica Sales
# MAGIC
# MAGIC Reads the bronze Delta table, applies the reusable transformations from the
# MAGIC `etl` package (column cleanup, NULL handling, deduplication and typing) and
# MAGIC writes a conformed silver Delta table. This notebook depends on
# MAGIC **`BRONZE_ingest`** having been executed first.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

BRONZE_TABLE = "default.bronze_orders"
SILVER_TABLE = "default.silver_orders"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load reusable transformation package

# COMMAND ----------

# In a Databricks repo the `etl` folder is available on the notebook path.
import sys

sys.path.insert(0, "/Workspace/Repos/databricks-pyspark-etl")

from etl import (
    build_bronze_schema,
    clean_columns,
    drop_duplicate_orders,
    enforce_schema,
    handle_null_rates,
    to_date_invalid_as_null,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Apply silver transformations

# COMMAND ----------

bronze_df = spark.table(BRONZE_TABLE)

# 1 -> Trim / lowercase / keep canonical columns
silver_df = clean_columns(bronze_df)

# 2 -> Parse dates, invalid dates become NULL
silver_df = to_date_invalid_as_null(silver_df, "order_date")

# 3 -> Enforce the canonical bronze schema (raises on required-column loss)
silver_df = enforce_schema(silver_df, build_bronze_schema())

# 4 -> NULL categorical columns -> 'unknown'
silver_df = handle_null_rates(silver_df, ["customer_id", "region", "status", "product_name", "category"])

# 5 -> De-duplicate on order_id (keeps first occurrence)
silver_df = drop_duplicate_orders(silver_df, subset=["order_id"])

print(f"Silver layer has {silver_df.count()} rows after cleansing")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Write the silver Delta table

# COMMAND ----------

if spark.catalog.tableExists(SILVER_TABLE):
    silver_df.write.mode("overwrite").saveAsTable(SILVER_TABLE)
else:
    silver_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(SILVER_TABLE)

display(silver_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC The cleansed, conformed silver table is ready. Run **`GOLD_report`** next.