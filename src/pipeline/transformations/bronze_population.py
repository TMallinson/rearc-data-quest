"""Bronze: DataUSA national population, each value's exact source text (D12, D31).

Every JSON parser normalises numbers (316128839.0 becomes 3.16128839E8 or 316128839),
so values are cut out of the raw text instead. Because pipeline functions mustn't
collect data, the extraction guards are fail expectations on a temporary view. The
published table then selects the clean columns from that view.
"""
from pyspark import pipelines as dp
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

POPULATION_FILE = spark.conf.get("rearc.raw_root") + "/datausa/population.json"

# Source key -> Bronze column. A relabel only; values are untouched (D29).
KEYS = {"Nation ID": "nation_id", "Nation": "nation", "Year": "year", "Population": "population"}
EXPECTED_KEYS_SQL = "array(" + ", ".join(f"'{key}'" for key in sorted(KEYS)) + ")"

DATA_ARRAY = r'(?s)"data"\s*:\s*\[(.*)\]'  # the text inside "data": [ ... ]
FLAT_OBJECT = r"\{[^{}]*\}"  # one record; records are flat (checked below)
JSON_KEY = r'"((?:[^"\\]|\\.)*)"\s*:'
JSON_VALUE = r'("(?:[^"\\]|\\.)*"|[^,}\s]+)'  # a quoted string, or a bare number/literal


@dp.temporary_view(name="population_records")
@dp.expect_or_fail("document_has_records", "record IS NOT NULL")
@dp.expect_or_fail("every_record_extracted", "coalesce(records_matched = records_in_document, false)")
@dp.expect_or_fail("record_keys_as_expected", f"coalesce(record_keys = {EXPECTED_KEYS_SQL}, false)")
def population_records() -> DataFrame:
    document = spark.read.format("text").option("wholetext", "true").load(POPULATION_FILE)
    data_text = F.regexp_extract("value", DATA_ARRAY, 1)
    return (
        document.select(
            # A real JSON parser's count of records, to check the text extraction against.
            F.expr("json_array_length(get_json_object(value, '$.data'))").alias("records_in_document"),
            F.regexp_extract_all(data_text, F.lit(FLAT_OBJECT), F.lit(0)).alias("records"),
            F.col("_metadata.file_path").alias("source_file"),
            F.col("_metadata.file_modification_time").alias("ingested_at"),
        )
        .select(
            "*",
            F.size("records").alias("records_matched"),
            F.explode_outer("records").alias("record"),  # explode_outer: no records still yields a row to fail on
        )
        .drop("records")
        .withColumn("record_keys", F.array_sort(F.regexp_extract_all("record", F.lit(JSON_KEY), F.lit(1))))
    )


def raw_value(key: str) -> Column:
    """The value's source text; for a JSON string, the characters inside the quotes."""
    text = F.regexp_extract("record", rf'"{key}"\s*:\s*' + JSON_VALUE, 1)
    return F.when(text.startswith('"'), text.substr(F.lit(2), F.length(text) - 2)).otherwise(text)


@dp.materialized_view(
    name="bronze_population",
    comment="DataUSA population records: each value's exact source text, keys renamed to snake_case (D31).",
)
def bronze_population() -> DataFrame:
    return spark.read.table("population_records").select(
        *[raw_value(key).alias(column) for key, column in KEYS.items()],
        "source_file",
        "ingested_at",
    )
