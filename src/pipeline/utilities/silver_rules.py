"""Silver rules as pure values and functions (no Spark), shared by pipeline code and pytest."""
from __future__ import annotations

ALLDATA_FILE = "pr.data.1.AllData"  # D30: Silver's only data input, matched exactly
CURRENT_FILE = "pr.data.0.Current"  # compared with AllData by the D6 check only

VALUE_PRECISION, VALUE_SCALE = 12, 3  # D33
VALUE_TYPE = f"DECIMAL({VALUE_PRECISION},{VALUE_SCALE})"

# No backslashes, so the patterns read the same in Python and inside SQL string literals.
NUMERIC_PATTERN = "^-?[0-9]+([.][0-9]+)?$"
SERIES_ID_PATTERN = "^PR[SU][0-9]{8}$"  # PR + seasonal code + sector(4) class(1) measure(2) duration(1)
WHOLE_NUMBER_PATTERN = "^[0-9]+([.]0+)?$"


def decimal_fits_pattern(precision: int, scale: int) -> str:
    """Text pattern for values that fit DECIMAL(precision, scale) without rounding (D16)."""
    return f"^-?[0-9]{{1,{precision - scale}}}([.][0-9]{{1,{scale}}})?$"


VALUE_FITS_PATTERN = decimal_fits_pattern(VALUE_PRECISION, VALUE_SCALE)


def base_name(path: str) -> str:
    """Base file name of a source_file path such as dbfs:/Volumes/.../pr.data.1.AllData."""
    return path.rsplit("/", 1)[-1]


def fallback_label(measure: str | None, sector: str | None, duration: str | None) -> str | None:
    """D5: the label built from lookup names when series_title is missing.

    Mirrors BLS's own title style, e.g. "Employment for the Manufacturing sector,
    % Change same quarter 1 year ago". None if any part is missing.
    """
    if not (measure and sector and duration):
        return None
    return f"{measure} for the {sector} sector, {duration}"
