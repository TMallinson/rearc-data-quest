"""scripts/volume_check.py must stay read-only: no upload, delete or directory changes."""
import re
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "volume_check.py"
WRITE_CALLS = re.compile(r"\.(upload|delete|create_directory|delete_directory|put|move|mkdirs)\(")


def test_volume_check_makes_no_write_calls():
    assert WRITE_CALLS.findall(SCRIPT.read_text()) == []
