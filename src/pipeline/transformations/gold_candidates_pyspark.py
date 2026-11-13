"""Gold answers, ALTERNATIVE implementations in PySpark (D37).

Private datasets: computed and compared with the SQL primaries in dq_gold_parity, but
not published. Column names, types, order and tie rules mirror gold_candidates.sql
exactly, so the parity check can compare whole rows.
"""
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

QUARTERS = ["Q01", "Q02", "Q03", "Q04"]


@dp.materialized_view(name="q1_candidate_pyspark", private=True, comment="Q1 alternative (PySpark).")
def q1_candidate_pyspark() -> DataFrame:
    return (
        spark.read.table("silver_population")
        .where(F.col("year").between(2013, 2018))
        .agg(
            F.count(F.lit(1)).alias("years"),
            F.min("year").alias("first_year"),
            F.max("year").alias("last_year"),
            F.avg("population").alias("mean_population"),
            F.stddev_pop("population").alias("stddev_population"),  # D2: the answer
            F.stddev_samp("population").alias("stddev_sample"),
        )
    )


@dp.materialized_view(name="q2_candidate_pyspark", private=True, comment="Q2 alternative (PySpark).")
def q2_candidate_pyspark() -> DataFrame:
    data = spark.read.table("silver_pr_data")
    is_quarter = F.col("period").isin(QUARTERS)

    # D3: annual-only if the data has no quarterly rows for the series.
    basis = data.groupBy("series_id").agg(
        F.when(F.count_if(is_quarter) == 0, "annual value").otherwise("quarterly sum").alias("basis")
    )
    year_totals = (
        data.join(basis, "series_id")
        .groupBy("series_id", "basis", "year")
        .agg(
            F.sum(
                F.when((F.col("basis") == "quarterly sum") & is_quarter, F.col("value"))
                .when((F.col("basis") == "annual value") & (F.col("period") == "Q05"), F.col("value"))
            ).alias("total"),
            F.count_if(is_quarter).alias("quarter_count"),
            F.count_if(F.col("period") == "Q05").alias("annual_count"),
        )
    )
    # D4: complete years only.
    complete = year_totals.where(
        ((F.col("basis") == "quarterly sum") & (F.col("quarter_count") == 4))
        | ((F.col("basis") == "annual value") & (F.col("annual_count") == 1))
    )
    best = complete.groupBy("series_id").agg(F.max("total").alias("best_total"))
    # D17: all complete years reaching the best total, sorted; the earliest wins.
    tied = (
        complete.join(best, "series_id")
        .where(F.col("total") == F.col("best_total"))
        .groupBy("series_id", "basis", "best_total")
        .agg(
            F.array_sort(F.collect_list("year")).alias("tied_years"),
            F.min_by("quarter_count", "year").alias("quarter_count"),
        )
    )
    labels = spark.read.table("silver_pr_series").select(
        "series_id", "label", "measure_text", "sector_name", "duration_text"
    )
    return tied.join(labels, "series_id", "left").select(
        "series_id", "label", "measure_text", "sector_name", "duration_text", "basis",
        F.element_at("tied_years", 1).alias("best_year"),
        "best_total",
        "quarter_count",
        F.slice("tied_years", 2, F.size("tied_years") - 1).alias("other_tied_years"),
    )


@dp.materialized_view(name="q3_candidate_pyspark", private=True, comment="Q3 alternative (PySpark).")
def q3_candidate_pyspark() -> DataFrame:
    data = spark.read.table("silver_pr_data").where(
        (F.col("series_id") == "PRS30006032") & (F.col("period") == "Q01")
    )
    population = spark.read.table("silver_population").select("year", "population")
    return data.join(population, "year", "left").select(  # D15: left join, INT = INT
        "series_id", "year", "period", "value", "population"
    )
