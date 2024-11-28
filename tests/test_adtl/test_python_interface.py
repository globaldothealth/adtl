from __future__ import annotations

from pathlib import Path

import adtl


def test_parse(snapshot):
    adtl.parse(
        "tests/test_adtl/parsers/epoch.json",
        "tests/test_adtl/sources/epoch.csv",
        output="output",
        encoding="utf-8",
    )
    assert Path("output-table.csv").read_text() == snapshot
    Path("output-table.csv").unlink()
