"""Shared pytest fixtures: a local SparkSession for the unit tests.

The session is created once per test module (module scope) to keep CI fast.
``SPARK_LOCAL_IP`` is set defensively so Spark can bind loopback on runners
where the default hostname resolution is unreliable.
"""
import os

import pytest
from pyspark.sql import SparkSession

os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[2]")
        .appName("medallion-etl-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()