"""Multi-row checks (D6, D12, D13).

An expectation sees one row at a time. So each rule that spans rows is first computed
into a check dataset whose rows carry the answer, and the expectation is attached to
that dataset. These are materialized views, not temporary views: a temporary view is
only computed when something reads it, and nothing reads a check.
"""
from functools import reduce

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from utilities.silver_rules import ALLDATA_FILE, CURRENT_FILE
from utilities.spark_helpers import base_name, trimmed

FIXED_KEY_DATASETS = ("silver_pr_data", "silver_pr_series", "silver_population")


@dp.materialized_view(name="dq_unique_keys", comment="One row per key per Silver dataset, with its row count (D13).")
@dp.expect_or_fail("data_key_unique", "dataset <> 'silver_pr_data' OR rows = 1")  # conflicting values
@dp.expect_or_fail("series_id_unique", "dataset <> 'silver_pr_series' OR rows = 1")
@dp.expect_or_fail("population_year_unique", "dataset <> 'silver_population' OR rows = 1")
@dp.expect_or_fail(
    "lookup_key_unique",  # a repeated lookup key would multiply rows in Silver's joins
    "dataset IN ('silver_pr_data', 'silver_pr_series', 'silver_population') OR rows = 1",
)
def dq_unique_keys() -> DataFrame:
    def key_counts(table: str, *keys: str) -> DataFrame:
        return (
            spark.read.table(table).groupBy(*keys).count()
            .select(
                F.lit(table).alias("dataset"),
                F.concat_ws("|", *[F.col(k).cast("string") for k in keys]).alias("key"),
                F.col("count").alias("rows"),
            )
        )

    return reduce(DataFrame.unionByName, [
        key_counts("silver_pr_data", "series_id", "year", "period"),
        key_counts("silver_pr_series", "series_id"),
        key_counts("silver_population", "year"),
        key_counts("silver_pr_period", "period"),
        key_counts("silver_pr_measure", "measure_code"),
        key_counts("silver_pr_sector", "sector_code"),
        key_counts("silver_pr_duration", "duration_code"),
        key_counts("silver_pr_footnote", "footnote_code"),
    ])


@dp.materialized_view(
    name="dq_current_in_alldata",
    comment="One row per Current row: is it in AllData, and with the same value? (D6)",
)
@dp.expect("current_row_in_alldata", "in_alldata")  # warn (D6)
@dp.expect("current_value_matches_alldata", "NOT in_alldata OR value_matches")  # missing rows counted once
def dq_current_in_alldata() -> DataFrame:
    bronze = spark.read.table("bronze_pr_data")
    keys = ["series_id", "year", "period"]

    def file_rows(file_name: str) -> DataFrame:
        return bronze.where(base_name() == file_name).select(*trimmed(keys + ["value"]))

    alldata = (
        file_rows(ALLDATA_FILE).withColumnRenamed("value", "alldata_value")
        .withColumn("in_alldata", F.lit(True))
    )
    return (
        file_rows(CURRENT_FILE).join(alldata, keys, "left")
        .select(
            *keys,
            F.col("value").alias("current_value"),
            "alldata_value",
            F.coalesce(F.col("in_alldata"), F.lit(False)).alias("in_alldata"),
            F.coalesce(F.col("value") == F.col("alldata_value"), F.lit(False)).alias("value_matches"),
        )
    )


@dp.materialized_view(name="dq_alldata_input", comment="Row count of Silver's AllData input (D12).")
@dp.expect_or_fail("alldata_not_empty", "alldata_rows > 0")  # an aggregate over 0 rows still yields 1 row
def dq_alldata_input() -> DataFrame:
    return (
        spark.read.table("bronze_pr_data").where(base_name() == ALLDATA_FILE)
        .agg(F.count(F.lit(1)).alias("alldata_rows"))
    )
