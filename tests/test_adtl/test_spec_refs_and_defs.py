from pathlib import Path

import pytest

import adtl.parser as parser

TEST_PARSERS_PATH = Path(__file__).parent / "parsers"


@pytest.mark.parametrize(
    "source,expected",
    [
        (({}, {}), {}),
        (
            (
                {"a": {"ref": "map"}, "b": 2},
                {"map": {"values": {"1": True, "2": False}}},
            ),
            {"a": {"values": {"1": True, "2": False}}, "b": 2},
        ),
        (
            (
                {"a": [{"ref": "map"}, {"x": 4}]},
                {"map": {"values": {"1": True, "2": False}}},
            ),
            {"a": [{"values": {"1": True, "2": False}}, {"x": 4}]},
        ),
    ],
)
def test_expand_refs(source, expected):
    assert parser.expand_refs(*source) == expected


def test_reference_expansion():
    ps_noref = parser.Parser(TEST_PARSERS_PATH / "groupBy.json")
    ps_ref = parser.Parser(TEST_PARSERS_PATH / "groupBy-defs.json")
    del ps_ref.spec["adtl"]["defs"]
    assert ps_ref.spec == ps_noref.spec


def test_reference_expansion_with_include():
    ps_noinclude = parser.Parser(TEST_PARSERS_PATH / "groupBy-defs.toml")
    ps_include = parser.Parser(TEST_PARSERS_PATH / "groupBy-defs-include.toml")
    del ps_noinclude.spec["adtl"]["defs"]
    del ps_include.spec["adtl"]["include-def"]
    assert ps_noinclude.spec == ps_include.spec


def test_external_definitions():
    with pytest.raises(KeyError):
        parser.Parser(TEST_PARSERS_PATH / "groupBy-external-defs.toml")
    ps = parser.Parser(
        TEST_PARSERS_PATH / "groupBy-external-defs.toml",
        include_defs=[TEST_PARSERS_PATH / "include-def.toml"],
    )
    assert ps.defs["sexMapping"]["values"] == {
        "1": "male",
        "2": "female",
        "3": "non_binary",
    }


@pytest.mark.parametrize(
    "source", [str(TEST_PARSERS_PATH / "apply.toml"), TEST_PARSERS_PATH / "epoch.json"]
)
def test_read_definition(source):
    assert parser.read_definition(source)


def test_validate_spec():
    with pytest.raises(ValueError, match="Specification header requires key"):
        _ = parser.Parser(dict())


@pytest.mark.parametrize(
    "source,expected",
    [
        (TEST_PARSERS_PATH / "oneToMany.json", ["observation"]),
        (TEST_PARSERS_PATH / "groupBy.json", ["subject"]),
    ],
)
def test_load_spec(source, expected):
    ps = parser.Parser(source)
    assert list(ps.tables.keys()) == expected


def test_unsupported_spec_format_raises_exception():
    with pytest.raises(ValueError, match="Unsupported file format"):
        parser.read_definition(TEST_PARSERS_PATH / "epoch.yml")
    with pytest.raises(ValueError, match="adtl specification format not supported"):
        parser.Parser(str(TEST_PARSERS_PATH / "epoch.yml"))


@pytest.mark.parametrize(
    "source,error",
    [
        (
            TEST_PARSERS_PATH / "groupBy-missing-kind.json",
            "Required 'kind' attribute within 'tables' not present for",
        ),
        (
            TEST_PARSERS_PATH / "groupBy-missing-table.json",
            "Parser specification missing required",
        ),
        (
            TEST_PARSERS_PATH / "groupBy-incorrect-aggregation.json",
            "groupBy needs 'aggregation' to be set for table:",
        ),
        (
            TEST_PARSERS_PATH / "oneToMany-missing-discriminator.json",
            "discriminator is required for 'oneToMany' tables",
        ),
    ],
    ids=[
        "missing-kind",
        "missing-table",
        "incorrect-aggregation",
        "missing-discriminator",
    ],
)
def test_invalid_spec_raises_error(source, error):
    with pytest.raises(ValueError, match=error):
        _ = parser.Parser(source)
