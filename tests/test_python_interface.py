from pathlib import Path

import adtl


def test_parse(snapshot):
    adtl.parse(
        "tests/parsers/epoch.json",
        "tests/sources/epoch.csv",
        output="output",
        encoding="utf-8",
    )
    assert Path("output-table.csv").read_text() == snapshot
    Path("output-table.csv").unlink()
