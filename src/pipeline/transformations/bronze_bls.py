"""Bronze: the BLS pr files exactly as ingested (D12, D13, D14).

One materialized view per file type. Each refresh re-reads the current files in the
raw Volume (C1), so changed and removed files are reflected without extra logic.

- Every data column is a string. Values aren't trimmed or unquoted.
- Header names are trimmed. The trimmed header must equal the expected list below,
  or the update fails.
- Metadata columns: source_file and ingested_at (when ingestion wrote the file).
"""
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from utilities.bronze_contract import check_header

BLS_DIR = spark.conf.get("rearc.raw_root") + "/bls/pr"

# Bronze's schema contract: table -> (file pattern, expected header after trimming).
# Taken from the files themselves; pr.txt (2011) is out of date (see notes/decisions.md).
BLS_FILES = {
    "bronze_pr_data": ("pr.data.*", ["series_id", "year", "period", "value", "footnote_codes"]),
    "bronze_pr_series": ("pr.series", [
        "series_id", "sector_code", "class_code", "measure_code", "duration_code", "seasonal",
        "base_year", "footnote_codes", "series_title", "begin_year", "begin_period", "end_year",
        "end_period",
    ]),
    "bronze_pr_class": ("pr.class", ["class_code", "class_text", "display_level", "selectable", "sort_sequence"]),
    "bronze_pr_duration": ("pr.duration", ["duration_code", "duration_text", "display_level", "selectable", "sort_sequence"]),
    "bronze_pr_footnote": ("pr.footnote", ["footnote_code", "footnote_text"]),
    "bronze_pr_measure": ("pr.measure", ["measure_code", "measure_text", "display_level", "selectable", "sort_sequence"]),
    "bronze_pr_period": ("pr.period", ["period", "period_abbr", "period_name"]),
    "bronze_pr_seasonal": ("pr.seasonal", ["Seasonal_code", "Seasonal_text"]),
    "bronze_pr_sector": ("pr.sector", ["sector_code", "sector_name", "display_level", "selectable", "sort_sequence"]),
}

RESCUED = "_rescued_data"


def read_bls_tsv(pattern: str, expected_header: list[str]) -> DataFrame:
    raw = (
        spark.read.format("csv")
        .option("sep", "\t")
        .option("header", "true")
        .option("inferSchema", "false")  # every column is a string
        .option("quote", "")  # no quote processing: values stay as written
        .option("ignoreLeadingWhiteSpace", "false")  # keep the padding (trimmed in Silver, D14)
        .option("ignoreTrailingWhiteSpace", "false")
        .option("rescuedDataColumn", RESCUED)  # malformed fields land here, not silently lost
        .load(f"{BLS_DIR}/{pattern}")
    )
    header = [name for name in raw.columns if name != RESCUED]
    trimmed = [name.strip() for name in header]
    # Header check (D13): fails the update. Expectations only see rows, so this runs in code.
    check_header(pattern, trimmed, expected_header)
    return raw.select(
        *[F.col(f"`{name}`").alias(clean) for name, clean in zip(header, trimmed)],
        F.col(RESCUED),
        F.col("_metadata.file_path").alias("source_file"),
        F.col("_metadata.file_modification_time").alias("ingested_at"),
    )


def define_bronze_table(table: str, pattern: str, expected_header: list[str]) -> None:
    # A factory, so each table's function captures its own pattern and header.
    @dp.materialized_view(
        name=table,
        comment=f"BLS {pattern} exactly as ingested: string columns, header names trimmed (D12).",
    )
    @dp.expect("no_rescued_data", f"{RESCUED} IS NULL")  # warn: Bronze keeps every row (D13)
    def bronze() -> DataFrame:
        return read_bls_tsv(pattern, expected_header)


for _table, (_pattern, _header) in BLS_FILES.items():
    define_bronze_table(_table, _pattern, _header)
