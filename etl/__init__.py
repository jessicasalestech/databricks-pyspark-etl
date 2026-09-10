"""etl - reusable PySpark transformation helpers for a Medallion pipeline.

Functional, "pure-ish" functions that operate on PySpark DataFrames so they can
be unit-tested against a local SparkSession in CI (GitHub Actions, JDK 17).
"""

from typing import List, Dict

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DecimalType, NumericType, StringType, StructField, StructType

# Columns that are guaranteed to exist after the bronze ingestion.
REQUIRED_BRONZE_COLUMNS: List[str] = [
    "order_id",
    "customer_id",
    "order_date",
    "product_name",
    "category",
    "quantity",
    "unit_price",
    "region",
    "status",
]

# Statuses considered a valid, saleable order for the gold layer.
ACTIVE_STATUSES: List[str] = ["completed", "pending", "refunded"]


def clean_columns(df: DataFrame, columns: List[str] = REQUIRED_BRONZE_COLUMNS) -> DataFrame:
    """Trim whitespace, lowercase and keep only the given column names.

    This normalises raw source data (which often arrives with stray
    whitespace or inconsistent casing) into a predictable silver schema.
    Any column missing from the source is not invented; we only work on the
    columns that actually exist.
    """
    existing = [c for c in columns if c in df.columns]
    cleaned = df.select(*existing)
    for col in existing:
        cleaned = cleaned.withColumn(
            col, F.trim(F.lower(F.col(col).cast(StringType())))
        )
    return cleaned


def handle_null_rates(df: DataFrame, columns: List[str]) -> DataFrame:
    """Fill NULLs in the given columns with a predictable sentinel.

    Missing categorical values (e.g. a blank ``region`` or ``customer_id``)
    are replaced with ``"unknown"`` so downstream joins and aggregations never
    silently drop rows.
    """
    for col in columns:
        if col in df.columns:
            df = df.withColumn(col, F.when(F.col(col).isNull(), "unknown").otherwise(F.col(col)))
    return df


def drop_duplicate_orders(df: DataFrame, subset: List[str] = ["order_id"]) -> DataFrame:
    """Remove duplicate rows keeping the first occurrence per ``subset`` key."""
    return df.dropDuplicates(subset)


def enforce_schema(df: DataFrame, schema: StructType) -> DataFrame:
    """Cast a DataFrame to the target schema, raising on missing required columns.

    Typing is explicit here so schema drift between batches is caught early in
    the silver layer rather than surfacing as a confusing failure at runtime.
    """
    for field in schema.fields:
        if field.name not in df.columns:
            raise ValueError(
                f"Missing required column '{field.name}' while enforcing schema."
            )
        current = df.schema[field.name].dataType
        desired = field.dataType
        if isinstance(desired, (NumericType, DateType)) and not isinstance(current, type(desired)):
            df = df.withColumn(field.name, F.col(field.name).cast(desired))
    return df.select([f.name for f in schema.fields])


def to_date_invalid_as_null(df: DataFrame, date_col: str, fmt: str = "yyyy-MM-dd") -> DataFrame:
    """Parse ``date_col`` with ``fmt``; unparseable values become NULL.

    Allows the pipeline to treat malformed dates as missing data instead of
    failing the whole batch.
    """
    return df.withColumn(date_col, F.to_date(F.col(date_col), fmt))


def compute_revenue_per_order(df: DataFrame) -> DataFrame:
    """Compute ``total_revenue = quantity * unit_price`` per row.

    NA values are exploded to 0 with coalesce so a single bad row cannot
    poison a group aggregation.
    """
    qty = F.col("quantity").cast("double")
    price = F.col("unit_price").cast("double")
    return df.withColumn(
        "total_revenue",
        F.coalesce(qty, F.lit(0.0)) * F.coalesce(price, F.lit(0.0)),
    )


def compute_aggregates(df: DataFrame, group_cols: List[str]) -> DataFrame:
    """Group by ``group_cols`` and produce revenue-aggregation metrics.

    Used by the gold layer to report revenue and volumes. Assumes the input
    already has a numeric ``total_revenue`` column (see ``compute_revenue_per_order``).
    """
    return (
        df.groupBy(*group_cols)
        .agg(
            F.count("*").alias("order_count"),
            F.sum("total_revenue").alias("total_revenue"),
            F.avg("total_revenue").alias("avg_order_value"),
        )
        .orderBy(F.desc("total_revenue"))
    )


def build_bronze_schema() -> StructType:
    """Canonical schema for the silver 'orders' table."""
    return StructType(
        [
            StructField("order_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("order_date", DateType(), True),
            StructField("product_name", StringType(), True),
            StructField("category", StringType(), True),
            StructField("quantity", StringType(), True),
            StructField("unit_price", StringType(), True),
            StructField("region", StringType(), True),
            StructField("status", StringType(), True),
        ]
    )


CATEGORY_REVENUE_GROUP: List[str] = ["category"]
REGION_REVENUE_GROUP: List[str] = ["region"]