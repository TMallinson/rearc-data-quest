"""Bronze header contract (D13): a pure function, so its failure paths are unit-tested.

The pipeline's root folder (src/pipeline) is on sys.path during updates, so the
pipeline imports this as `utilities.bronze_contract`. pytest adds the same folder.
"""
from __future__ import annotations


class HeaderMismatchError(ValueError):
    """A file's trimmed header differs from the expected columns. Fails the update."""


def check_header(source: str, actual: list[str], expected: list[str]) -> None:
    """Raise HeaderMismatchError naming the file and the difference, unless headers match."""
    if actual == expected:
        return
    missing = [name for name in expected if name not in actual]
    unexpected = [name for name in actual if name not in expected]
    if len(actual) == len(expected) and len(missing) == 1 and len(unexpected) == 1:
        difference = f"renamed column {missing[0]!r} -> {unexpected[0]!r}"
    elif missing or unexpected:
        parts = []
        if missing:
            parts.append(f"missing {missing}")
        if unexpected:
            parts.append(f"unexpected {unexpected}")
        difference = "; ".join(parts)
    else:
        difference = "columns reordered"
    raise HeaderMismatchError(
        f"Bronze header check failed for {source}: {difference}. "
        f"Got {actual}, expected {expected}."
    )
