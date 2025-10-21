import collections
import contextlib
import io
import json
from pathlib import Path
from typing import Any, Dict, Iterable

import pytest
import responses

import adtl

TEST_PARSERS_PATH = Path(__file__).parent / "parsers"
TEST_SOURCES_PATH = Path(__file__).parent / "sources"
TEST_SCHEMAS_PATH = Path(__file__).parent / "schemas"

ARGV = [
    str(TEST_PARSERS_PATH / "epoch.json"),
    str(TEST_SOURCES_PATH / "epoch.csv"),
    "-o",
    "output",
    "--encoding",
    "utf-8",
]


def test_main(snapshot):
    adtl.main(ARGV)
    assert Path("output-table.csv").read_text() == snapshot
    Path("output-table.csv").unlink()


def test_main_parquet():
    adtl.main(ARGV + ["--parquet"])
    assert Path("output-table.parquet")
    Path("output-table.parquet").unlink()


def test_main_parquet_error():
    ARG = [
        str(TEST_PARSERS_PATH / "return-unmapped.toml"),
        str(TEST_SOURCES_PATH / "return-unmapped.csv"),
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
def test_main_web_schema(snapshot):
    # test with schema on the web
    epoch_schema = json.loads(
        Path(TEST_SCHEMAS_PATH / "epoch-data.schema.json").read_text()
    )
    responses.add(
        responses.GET,
        "http://example.com/schemas/epoch-data.schema.json",
        json=epoch_schema,
        status=200,
    )
    adtl.main([str(TEST_PARSERS_PATH / "epoch-web-schema.json")] + ARGV[1:])
    assert Path("output-table.csv").read_text() == snapshot
    Path("output-table.csv").unlink()


@responses.activate
def test_main_web_schema_missing(snapshot):
    responses.add(
        responses.GET,
        "http://example.com/schemas/epoch-data.schema.json",
        json={"error": "not found"},
        status=404,
    )
    adtl.main([str(TEST_PARSERS_PATH / "epoch-web-schema.json")] + ARGV[1:])
    assert Path("output-table.csv").read_text() == snapshot
    Path("output-table.csv").unlink()


def _subdict(d: Dict, keys: Iterable[Any]) -> Dict[str, Any]:
    return {k: d.get(k) for k in keys}


def test_main_save_report():
    adtl.main(ARGV + ["--save-report", "epoch-report.json"])
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


def test_show_report(snapshot):
    ps = adtl.parser.Parser(TEST_PARSERS_PATH / "epoch.json")
    ps.report = {
        "total": {"table": 10},
        "total_valid": {"table": 8},
        "validation_errors": {
            "table": collections.Counter(
                [
                    "data must be valid exactly by one definition (0 matches found)",
                    "data must contain ['epoch'] properties",
                ]
            )
        },
    }
    ps.report_available = True
    with contextlib.redirect_stdout(io.StringIO()) as f:
        ps.show_report()
    assert f.getvalue() == snapshot
