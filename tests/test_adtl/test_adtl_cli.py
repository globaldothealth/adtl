import json
from pathlib import Path
from typing import Any, Dict, Iterable

import pytest
import responses
from shared import parser_path, schemas_path, sources_path

import adtl.cli as adtl

ARGV_PARSE = [
    "parse",
    str(parser_path / "epoch.json"),
    str(sources_path / "epoch.csv"),
    "-o",
    "output",
    "--encoding",
    "utf-8",
]


def test_parse_main(snapshot):
    adtl.main(ARGV_PARSE)
    assert Path("output-table.csv").read_text() == snapshot
    Path("output-table.csv").unlink()


def test_parse_main_parquet():
    adtl.main(ARGV_PARSE + ["--parquet"])
    assert Path("output-table.parquet")
    Path("output-table.parquet").unlink()


def test_parse_parquet_error():
    ARG = [
        "parse",
        str(parser_path / "return-unmapped.toml"),
        str(sources_path / "return-unmapped.csv"),
        "-o",
        "output",
        "--encoding",
        "utf-8",
    ]

    with pytest.raises(
        ValueError, match="returnUnmatched and parquet options are incompatible"
    ):
        adtl.main(ARG + ["--parquet"])


@responses.activate
def test_parse_web_schema(snapshot):
    # test with schema on the web
    epoch_schema = json.loads(Path(schemas_path / "epoch-data.schema.json").read_text())
    responses.add(
        responses.GET,
        "http://example.com/schemas/epoch-data.schema.json",
        json=epoch_schema,
        status=200,
    )
    adtl.main(["parse", str(parser_path / "epoch-web-schema.json")] + ARGV_PARSE[2:])
    assert Path("output-table.csv").read_text() == snapshot
    Path("output-table.csv").unlink()


@responses.activate
def test_parse_web_schema_missing(snapshot):
    responses.add(
        responses.GET,
        "http://example.com/schemas/epoch-data.schema.json",
        json={"error": "not found"},
        status=404,
    )
    adtl.main(["parse", str(parser_path / "epoch-web-schema.json")] + ARGV_PARSE[2:])
    assert Path("output-table.csv").read_text() == snapshot
    Path("output-table.csv").unlink()


def _subdict(d: Dict, keys: Iterable[Any]) -> Dict[str, Any]:
    return {k: d.get(k) for k in keys}


def test_parse_save_report():
    adtl.main(ARGV_PARSE + ["--save-report", "epoch-report.json"])
    report = json.loads(Path("epoch-report.json").read_text())
    assert report["file"].endswith("tests/test_adtl/sources/epoch.csv")
    assert report["parser"].endswith("tests/test_adtl/parsers/epoch.json")
    assert _subdict(
        report,
        ["encoding", "include_defs", "total", "total_valid", "validation_errors"],
    ) == {
        "encoding": "utf-8",
        "include_defs": [],
        "total": {"table": 2},
        "total_valid": {"table": 2},
        "validation_errors": {},
    }
    Path("epoch-report.json").unlink()
