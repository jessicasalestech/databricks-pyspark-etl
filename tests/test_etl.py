"""Tests for the reusable PySpark transformation helpers in the etl package.

Each test builds a tiny DataFrame with an explicit schema and asserts on the
transformed schema/rows, so the medallion logic is verifiable without a real
Databricks cluster.
"""
from datetime import date

from pyspark.sql import functions as F
from pyspark.sql.types import DateType, StringType, StructField, StructType

from etl import (
    clean_columns,
    compute_aggregates,
    compute_revenue_per_order,
    drop_duplicate_orders,
    enforce_schema,
    handle_null_rates,
    to_date_invalid_as_null,
)


def make_orders(spark):
    return spark.createDataFrame(
        [
            ("1001", "C001", "2024-01-05", "Wireless Mouse", "Electronics", 2, 25.5, "North", "completed"),
            ("1001", "C001", "2024-01-05", "Wireless Mouse", "Electronics", 2, 25.5, "North", "completed"),
            ("1002", None, "  ", "USB-C Cable", "Electronics", 5, 9.99, "South", "completed"),
            ("1003", "C002", "not-a-date", "Desk Lamp", "Home", 1, 45.0, None, "pending"),
            (None, "C003", "2024-01-08", "Bluetooth Speaker", "Electronics", 1, None, "West", "cancelled"),
        ],
        schema=[
            "order_id", "customer_id", "order_date", "product_name",
            "category", "quantity", "unit_price", "region", "status",
        ],
    )


def test_clean_columns_trim_lowercases_and_selects(spark):
    df = make_orders(spark)
    result = clean_columns(df, ["order_id", "product_name", "category", "guest_col"])
    assert result.columns == ["order_id", "product_name", "category"]
    assert result.collect()[0]["product_name"] == "wireless mouse"
    assert result.collect()[0]["category"] == "electronics"


def test_clean_columns_ignores_nonexistent_columns(spark):
    df = make_orders(spark)
    result = clean_columns(df, ["nope", "order_id"])
    assert result.columns == ["order_id"]


def test_to_date_invalid_as_null_parses_good_and_nulls_bad(spark):
    df = make_orders(spark)
    result = to_date_invalid_as_null(df, "order_date").select("order_date")
    rows = {r["order_date"] for r in result.collect()}
    assert date(2024, 1, 5) in rows
    assert None in rows  # "not-a-date" and blank both become NULL


def test_drop_duplicate_orders_keeps_first_unique_order_id(spark):
    df = make_orders(spark)
    result = drop_duplicate_orders(df, subset=["order_id"])
    ids = [r["order_id"] for r in result.select("order_id").orderBy("order_id").collect()]
    # 1001 appears only once, NULL (two rows) survives once after dedup
    assert ids.count("1001") == 1
    assert ids.count(None) == 1
    assert len(ids) == 4


def test_handle_null_rates_fills_remaining_blanks_with_unknown(spark):
    df = make_orders(spark)
    result = handle_null_rates(df, ["customer_id", "region", "status"])
    # No leftover NULL or empty-string categorical values remain
    bad = result.filter(F.col("customer_id").isin([""]) | F.col("customer_id").isNull()).count()
    assert bad == 0
    # The single blank customer_id was converted to the 'unknown' sentinel
    assert result.filter(F.col("customer_id") == "unknown").count() == 1
    # A NULL order_id is *not* a categorical blank, it stays NULL
    assert result.filter(F.col("order_id").isNull()).count() == 1


def test_enforce_schema_casts_columns(spark):
    df = make_orders(spark)
    result = enforce_schema(df, __schema())
    assert isinstance(result.schema["order_date"].dataType, DateType)
    # None / invalid dates cast to NULL Dates
    assert result.filter(F.col("order_date").isNull()).count() >= 1


def test_enforce_schema_raises_on_missing_required_column(spark):
    df = make_orders(spark).drop("status")
    schema = __schema()
    try:
        enforce_schema(df, schema)
        raise AssertionError("Expected ValueError for missing required column")
    except ValueError as exc:
        assert "Missing required column" in str(exc)


def test_compute_revenue_per_order_multiplication(spark):
    df = make_orders(spark)
    result = compute_revenue_per_order(df)
    first = result.filter(F.col("order_id") == "1001").select("total_revenue").first()
    assert first["total_revenue"] == 2 * 25.5
    # unit_price NULL -> revenue 0, never NULL/NaN
    null_price = result.filter(F.col("order_id").isNull()).select("total_revenue").first()
    assert null_price["total_revenue"] == 0.0


def test_compute_aggregates_grouped_revenue_is_correct(spark):
    df = make_orders(spark)
    with_revenue = compute_revenue_per_order(df)
    agg = compute_aggregates(with_revenue, ["category"])
    electronics = agg.filter(F.col("category") == "Electronics").first()
    # 1001(25.5*2)*2 + 1002(9.99*5) + speaker badly-priced(0) = 51 + 49.95 = 100.95
    assert abs(electronics["total_revenue"] - (25.5 * 2 * 2 + 9.99 * 5)) < 1e-6
    assert electronics["order_count"] == 4


def __schema():
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