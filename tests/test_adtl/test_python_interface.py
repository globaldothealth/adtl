from __future__ import annotations

from pathlib import Path

import pytest

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


@pytest.mark.parametrize(
    "spec",
    [
        "tests/test_adtl/parsers/epoch.json",
        "tests/test_adtl/parsers/apply.toml",
        {
            "adtl": {
                "name": "constant",
                "description": "Fixed table",
                "tables": {"metadata": {"kind": "constant"}},
            },
            "metadata": {
                "dataset": "constant",
                "version": "20220505.1",
                "format": "csv",
            },
        },
    ],
    ids=["json", "toml", "dict"],
)
def test_validate_specification(spec):
    adtl.validate_specification(
        spec,
    )
