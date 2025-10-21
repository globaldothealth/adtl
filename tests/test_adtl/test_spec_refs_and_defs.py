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


def test_unsupported_format_raises_exception():
    with pytest.raises(ValueError, match="Unsupported file format"):
        parser.read_definition(TEST_PARSERS_PATH / "epoch.yml")
    with pytest.raises(ValueError, match="adtl specification format not supported"):
        parser.Parser(str(TEST_PARSERS_PATH / "epoch.yml"))
