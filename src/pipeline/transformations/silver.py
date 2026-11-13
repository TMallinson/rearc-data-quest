"""Silver: typed, trimmed, deduplicated data from AllData only, with labels joined in.

Decisions: D3/D4 ranking logic is Gold's, so Silver keeps every period including Q05;
D5 labels; D6 (Silver reads AllData only); D13 expectations; D14 trimming; D15 integer
year; D16/D33 DECIMAL(12,3); D30 exact AllData file match.

Expectations that need helper columns (raw text, flags) sit on temporary views. The
published tables select the clean columns from those views.
"""
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from utilities.silver_rules import (
    ALLDATA_FILE, NUMERIC_PATTERN, SERIES_ID_PATTERN, VALUE_FITS_PATTERN, VALUE_TYPE,
    WHOLE_NUMBER_PATTERN,
)
from utilities.spark_helpers import base_name, fallback_label, trimmed

# ---------------------------------------------------------------- lookups
LOOKUPS = {
    "silver_pr_period": ("bronze_pr_period", ["period", "period_abbr", "period_name"]),
    "silver_pr_measure": ("bronze_pr_measure", ["measure_code", "measure_text"]),
    "silver_pr_sector": ("bronze_pr_sector", ["sector_code", "sector_name"]),
    "silver_pr_duration": ("bronze_pr_duration", ["duration_code", "duration_text"]),
    "silver_pr_footnote": ("bronze_pr_footnote", ["footnote_code", "footnote_text"]),
}


def define_lookup(table: str, source: str, columns: list[str]) -> None:
    @dp.materialized_view(name=table, comment=f"{source}, trimmed: code and text columns (D14).")
    @dp.expect_or_fail("key_present", f"{columns[0]} IS NOT NULL")  # a NULL key can't be joined
    def lookup() -> DataFrame:
        return spark.read.table(source).select(*trimmed(columns))


for _table, (_source, _columns) in LOOKUPS.items():
    define_lookup(_table, _source, _columns)


# ---------------------------------------------------------------- series
SERIES_COLUMNS = [
    "series_id", "sector_code", "class_code", "measure_code", "duration_code", "seasonal",
    "base_year", "footnote_codes", "series_title", "begin_year", "begin_period", "end_year",
    "end_period",
]


@dp.materialized_view(name="silver_pr_series", comment="pr.series typed, with decoded names and a label (D5).")
@dp.expect_or_fail("series_id_valid", f"coalesce(series_id RLIKE '{SERIES_ID_PATTERN}', false)")
@dp.expect("series_title_present", "series_title IS NOT NULL")  # warn: the fallback label covers it (D13)
@dp.expect("label_present", "label IS NOT NULL")  # warn: neither title nor lookups gave a label
def silver_pr_series() -> DataFrame:
    series = spark.read.table("bronze_pr_series").select(*trimmed(SERIES_COLUMNS))
    names = (
        series.join(spark.read.table("silver_pr_measure"), "measure_code", "left")
        .join(spark.read.table("silver_pr_sector"), "sector_code", "left")
        .join(spark.read.table("silver_pr_duration"), "duration_code", "left")
    )
    built = fallback_label()
    return names.select(
        "series_id",
        F.coalesce(F.col("series_title"), built).alias("label"),
        F.when(F.col("series_title").isNotNull(), "series_title")
        .when(built.isNotNull(), "built from lookups")
        .alias("label_source"),
        "series_title", "measure_code", "measure_text", "sector_code", "sector_name",
        "duration_code", "duration_text", "class_code",
        F.col("seasonal").alias("seasonal_code"),
        F.expr("try_cast(nullif(base_year, '-') AS INT)").alias("base_year"),  # '-' means no base year
        "footnote_codes",
        F.expr("try_cast(begin_year AS INT)").alias("begin_year"), "begin_period",
        F.expr("try_cast(end_year AS INT)").alias("end_year"), "end_period",
    )


# ---------------------------------------------------------------- data
DATA_COLUMNS = ["series_id", "year", "period", "value", "footnote_codes"]


@dp.temporary_view(name="silver_pr_data_staged")
# Drops. Conditions are mutually exclusive, so each dropped row is counted once.
@dp.expect_or_drop("row_not_blank", "NOT is_blank")  # known artifact (D13)
@dp.expect_or_drop("value_numeric", f"is_blank OR coalesce(value_text RLIKE '{NUMERIC_PATTERN}', false)")  # safe: D4
# Fails: skip rows that are being dropped, so a drop never turns into a failure.
@dp.expect_or_fail("series_id_valid", f"is_blank OR coalesce(series_id RLIKE '{SERIES_ID_PATTERN}', false)")
@dp.expect_or_fail("year_valid", "is_blank OR coalesce(year BETWEEN 1900 AND year(current_date()) + 1, false)")
@dp.expect_or_fail("period_known", "is_blank OR period_name IS NOT NULL")
@dp.expect_or_fail(
    "value_fits_decimal",  # D16: numeric but too long must fail, never be dropped or rounded
    f"is_blank OR NOT coalesce(value_text RLIKE '{NUMERIC_PATTERN}', false) "
    f"OR coalesce(value_text RLIKE '{VALUE_FITS_PATTERN}', false)",
)
# Warnings: keep the row (Q2 needs every series in the data).
@dp.expect("series_in_catalog", "is_blank OR series_known")
@dp.expect("footnotes_known", "footnotes_known")
def silver_pr_data_staged() -> DataFrame:
    alldata = spark.read.table("bronze_pr_data").where(base_name() == ALLDATA_FILE)  # D30
    rows = (
        alldata.select(*trimmed(DATA_COLUMNS), "ingested_at")
        .dropDuplicates(DATA_COLUMNS)  # exact duplicates (D13); NULLs compare equal here
        .withColumnRenamed("year", "year_text")
        .withColumnRenamed("value", "value_text")
    )
    series = spark.read.table("silver_pr_series").select(
        "series_id", "label", "measure_text", "sector_name", "duration_text",
        F.lit(True).alias("series_known"),
    )
    periods = spark.read.table("silver_pr_period").select("period", "period_name")
    known_footnotes = spark.read.table("silver_pr_footnote").agg(
        F.collect_set("footnote_code").alias("known_footnotes")
    )
    return (
        rows.join(series, "series_id", "left")
        .join(periods, "period", "left")
        .crossJoin(known_footnotes)  # one row: the set of valid footnote codes
        .select(
            "series_id",
            F.expr("try_cast(year_text AS INT)").alias("year"),  # D15
            "period", "period_name", "value_text",
            F.expr(f"try_cast(value_text AS {VALUE_TYPE})").alias("value"),
            "footnote_codes", "label", "measure_text", "sector_name", "duration_text",
            F.coalesce(F.col("series_known"), F.lit(False)).alias("series_known"),
            F.expr(
                "footnote_codes IS NULL OR "
                "forall(split(footnote_codes, ','), c -> array_contains(known_footnotes, trim(c)))"
            ).alias("footnotes_known"),
            F.expr(
                "series_id IS NULL AND year_text IS NULL AND period IS NULL "
                "AND value_text IS NULL AND footnote_codes IS NULL"
            ).alias("is_blank"),
            "ingested_at",
        )
    )


@dp.materialized_view(
    name="silver_pr_data",
    comment="AllData observations: typed (year INT, value DECIMAL(12,3)), trimmed, deduplicated, labelled.",
)
def silver_pr_data() -> DataFrame:
    return spark.read.table("silver_pr_data_staged").select(
        "series_id", "year", "period", "period_name", "value", "footnote_codes",
        "label", "measure_text", "sector_name", "duration_text", "ingested_at",
    )


# ---------------------------------------------------------------- population
@dp.temporary_view(name="silver_population_staged")
@dp.expect_or_fail("year_valid", "year IS NOT NULL")
@dp.expect_or_fail(
    "population_whole_positive",
    f"coalesce(population_text RLIKE '{WHOLE_NUMBER_PATTERN}' AND population > 0, false)",
)
@dp.expect_or_fail("nation_is_united_states", "coalesce(nation_id = '01000US' AND nation = 'United States', false)")
def silver_population_staged() -> DataFrame:
    population = spark.read.table("bronze_population").select(
        *trimmed(["nation_id", "nation", "year", "population"]), "ingested_at"
    )
    return population.select(
        "nation_id", "nation",
        F.expr("try_cast(year AS INT)").alias("year"),  # D15: joins to BLS year as INT = INT
        F.col("population").alias("population_text"),
        # Whole number checked above, so the integer part is the exact value.
        F.expr("try_cast(regexp_extract(population, '^([0-9]+)', 1) AS BIGINT)").alias("population"),
        "ingested_at",
    )


@dp.materialized_view(name="silver_population", comment="US population by year: year INT, population BIGINT.")
def silver_population() -> DataFrame:
    return spark.read.table("silver_population_staged").select(
        "year", "population", "nation_id", "nation", "ingested_at"
    )
