# Databricks notebook source
# MAGIC %md
# MAGIC # GOLD — Aggregated Reporting
# MAGIC
# MAGIC **Layer:** Gold (aggregated)
# MAGIC **Owner:** Jessica Sales
# MAGIC
# MAGIC Reads the conformed silver table, computes revenue per order, drops
# MAGIC cancelled/refused transactions and writes business-facing aggregations for
# MAGIC BI tooling. Depends on **`SILVER_transform`** having been run first.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

SILVER_TABLE = "default.silver_orders"
GOLD_CATEGORY_TABLE = "default.gold_category_revenue"
GOLD_REGION_TABLE = "default.gold_region_revenue"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load silver, compute revenue and restrict to active orders

# COMMAND ----------

import sys

sys.path.insert(0, "/Workspace/Repos/databricks-pyspark-etl")

from etl import ACTIVE_STATUSES, CATEGORY_REVENUE_GROUP, REGION_REVENUE_GROUP
from etl import compute_aggregates, compute_revenue_per_order

from pyspark.sql import functions as F

silver_df = spark.table(SILVER_TABLE)

# Full revenue per order line
with_revenue = compute_revenue_per_order(silver_df)

# Only orders that represent genuine sales (exclude cancelled)
active = with_revenue.filter(F.col("status").isin(*ACTIVE_STATUSES))
print(f"Active orders considered for reporting: {active.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Build gold aggregations

# COMMAND ----------

category_gold = compute_aggregates(active, CATEGORY_REVENUE_GROUP)
region_gold = compute_aggregates(active, REGION_REVENUE_GROUP)

if spark.catalog.tableExists(GOLD_CATEGORY_TABLE):
    category_gold.write.mode("overwrite").saveAsTable(GOLD_CATEGORY_TABLE)
else:
    category_gold.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(GOLD_CATEGORY_TABLE)

if spark.catalog.tableExists(GOLD_REGION_TABLE):
    region_gold.write.mode("overwrite").saveAsTable(GOLD_REGION_TABLE)
else:
    region_gold.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(GOLD_REGION_TABLE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Inspect results

# COMMAND ----------

print("=== Revenue by category ===")
display(category_gold)

print("=== Revenue by region ===")
display(region_gold)

# COMMAND ----------

# MAGIC %md
# MAGIC Gold reporting tables are ready for Power BI / Tableau / Databricks SQL.