"""Small Spark column helpers shared by Silver and the check datasets."""
from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F


def trimmed(columns: list[str]) -> list[Column]:
    """D14: trim every string; a value that is empty after trimming becomes NULL."""
    return [F.expr(f"nullif(trim(`{name}`), '')").alias(name) for name in columns]


def base_name(column: str = "source_file") -> Column:
    """Base file name of a path column; Spark twin of silver_rules.base_name."""
    return F.regexp_extract(F.col(column), r"([^/]+)$", 1)


def fallback_label() -> Column:
    """Spark twin of silver_rules.fallback_label: NULL if any part is NULL (concat is NULL-strict)."""
    return F.concat(
        F.col("measure_text"), F.lit(" for the "), F.col("sector_name"), F.lit(" sector, "),
        F.col("duration_text"),
    )
