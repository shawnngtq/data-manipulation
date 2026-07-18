"""Tests for data_manipulation.pyspark_.

All tests are marked ``spark`` and use the session-scoped ``spark`` fixture,
which skips the whole module when PySpark is not installed.
"""

import pytest

pytestmark = pytest.mark.spark

pytest.importorskip("pyspark")

from data_manipulation import pyspark_  # noqa: E402


def test_add_dummy_columns(spark):
    df = spark.createDataFrame([("Alice", 1)], ["name", "id"])
    out = pyspark_.add_dummy_columns(df, ["age", "city"], "unknown")
    assert set(out.columns) == {"name", "id", "age", "city"}
    row = out.collect()[0]
    assert row["age"] == "unknown" and row["city"] == "unknown"


def test_column_into_list(spark):
    df = spark.createDataFrame([(1,), (2,), (2,)], ["value"])
    assert pyspark_.column_into_list(df, "value") == [1, 2, 2]


def test_column_into_list_missing_column(spark):
    df = spark.createDataFrame([(1,)], ["value"])
    with pytest.raises(ValueError):
        pyspark_.column_into_list(df, "nope")


def test_columns_prefix(spark):
    df = spark.createDataFrame([("Alice", 1)], ["name", "id"])
    out = pyspark_.columns_prefix(df, "user_")
    assert out.columns == ["user_name", "user_id"]


def test_group_count(spark):
    df = spark.createDataFrame(
        [(1, "A"), (1, "B"), (2, "A")], ["id", "category"]
    )
    out = pyspark_.group_count(df, ["id"])
    assert {"count", "percent"}.issubset(set(out.columns))
    counts = {r["id"]: r["count"] for r in out.collect()}
    assert counts == {1: 2, 2: 1}


def test_describe_returns_none(spark):
    df = spark.createDataFrame([("Alice", 1)], ["name", "id"])
    assert pyspark_.describe(df) is None


def test_add_dummy_columns_bad_type(spark):
    df = spark.createDataFrame([("Alice", 1)], ["name", "id"])
    with pytest.raises(TypeError):
        pyspark_.add_dummy_columns(df, "not-a-list", "x")


def test_config_spark_local_recommendation_only():
    # autoset=False only logs a recommended config; it builds no session.
    assert pyspark_.config_spark_local(autoset=False) is None
