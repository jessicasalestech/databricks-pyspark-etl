"""Local smoke test of the chained Bronze -> Silver -> Gold ETL logic.

Runs the same reusable functions the Databricks notebooks use, against the
sample CSV, printing the gold aggregations. Executable directly:

    python tests/run_pipeline_demo.py
"""
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from etl import (  # noqa: E402
    ACTIVE_STATUSES,
    CATEGORY_REVENUE_GROUP,
    REGION_REVENUE_GROUP,
    build_bronze_schema,
    clean_columns,
    compute_aggregates,
    compute_revenue_per_order,
    drop_duplicate_orders,
    enforce_schema,
    handle_null_rates,
    to_date_invalid_as_null,
)

ROOT = Path(__file__).resolve().parent.parent
CSV = ROOT / "data" / "raw_orders_sample.csv"

os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")


def main():
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("medallion-demo")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    # ---- Bronze: raw read ----
    bronze = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("mode", "PERMISSIVE")
        .load(str(CSV))
    )
    print(f"[BRONZE] loaded {bronze.count()} raw rows")

    # ---- Silver: clean + conform (mirrors SILVER_transform.py) ----
    silver = clean_columns(bronze)
    silver = to_date_invalid_as_null(silver, "order_date")
    silver = enforce_schema(silver, build_bronze_schema())
    silver = handle_null_rates(silver, ["customer_id", "region", "status", "product_name", "category"])
    silver = drop_duplicate_orders(silver, subset=["order_id"])
    print(f"[SILVER] {silver.count()} cleansed rows")

    # ---- Gold: revenue + aggregations (mirrors GOLD_report.py) ----
    with_revenue = compute_revenue_per_order(silver)
    active = with_revenue.filter(F.col("status").isin(*ACTIVE_STATUSES))
    print(f"[GOLD] {active.count()} active orders considered")

    print("\nRevenue by category:")
    compute_aggregates(active, CATEGORY_REVENUE_GROUP).show(100, truncate=False)
    print("Revenue by region:")
    compute_aggregates(active, REGION_REVENUE_GROUP).show(100, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()