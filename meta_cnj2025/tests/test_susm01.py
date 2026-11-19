import pytest
from datetime import datetime, date
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, LongType, TimestampType, IntegerType


@pytest.fixture(scope="module")
def spark():
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("test_susm01")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield spark
    spark.stop()


def compute_susm_count(spark, situacao_88_rows, situacao_25_rows, period_start, period_end):
    schema = StructType([
        StructField("coddoc", LongType(), True),
        StructField("dthrmov", TimestampType(), True),
        StructField("dth_fim", TimestampType(), True),
        StructField("situacao", IntegerType(), True),
    ])

    df_88 = spark.createDataFrame(situacao_88_rows, schema=schema)
    df_25 = spark.createDataFrame(situacao_25_rows, schema=schema)

    latest_window = Window.partitionBy("coddoc").orderBy(col("dthrmov").desc())

    last_88 = df_88.withColumn("_rn", row_number().over(latest_window)).filter(col("_rn") == 1).drop("_rn")
    last_25 = df_25.withColumn("_rn", row_number().over(latest_window)).filter(col("_rn") == 1).drop("_rn")

    # last_25 closed in period
    last_25_closed_in_period = last_25.filter(
        (col("dth_fim").isNotNull()) &
        (year(col("dth_fim")) == period_end.year) &
        (col("dth_fim") >= period_start) & (col("dth_fim") <= period_end)
    )

    last_88_open = last_88.filter(col("dth_fim").isNull())

    susm_df = last_25_closed_in_period.join(last_88_open.select("coddoc"), "coddoc", "inner").dropDuplicates(["coddoc"])
    return susm_df.count(), susm_df


def test_scenario_not_counted(spark):
    # Scenario you gave: final state at month end -> 88 closed, 25 open
    coddoc = 123456
    period_start = datetime(2024, 12, 1)
    period_end = datetime(2024, 12, 31)

    # 88 movements: opened 28/12 (open), closed 29/12 (so latest is closed)
    situacao_88_rows = [
        (coddoc, datetime(2024, 12, 28, 10, 0), None, 88),
        (coddoc, datetime(2024, 12, 29, 12, 0), datetime(2024, 12, 29, 12, 0), 88),
    ]

    # 25 movements: open+close on 30/12, then opened again 31/12 (latest open)
    situacao_25_rows = [
        (coddoc, datetime(2024, 12, 30, 8, 0), datetime(2024, 12, 30, 9, 0), 25),
        (coddoc, datetime(2024, 12, 30, 10, 0), None, 25),  # an open later same day
        (coddoc, datetime(2024, 12, 31, 11, 0), None, 25),  # latest open at month end
    ]

    count, df = compute_susm_count(spark, situacao_88_rows, situacao_25_rows, period_start, period_end)
    assert count == 0


def test_scenario_counted(spark):
    # Variation: last 88 is open at period end and last 25 was closed in the period -> should be counted
    coddoc = 999999
    period_start = datetime(2024, 12, 1)
    period_end = datetime(2024, 12, 31)

    # 88 movements: opened 28/12 (open), and no closing later -> latest is open
    situacao_88_rows = [
        (coddoc, datetime(2024, 12, 28, 10, 0), None, 88),
    ]

    # 25 movements: closed on 30/12 and no later reopening -> latest closed in period
    situacao_25_rows = [
        (coddoc, datetime(2024, 12, 30, 8, 0), datetime(2024, 12, 30, 9, 0), 25),
    ]

    count, df = compute_susm_count(spark, situacao_88_rows, situacao_25_rows, period_start, period_end)
    assert count == 1
