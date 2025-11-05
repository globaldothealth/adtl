import pytest
from shared import parser_path

import adtl.parser as parser


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
    ids=["empty", "simple_ref", "list_ref"],
)
def test_expand_refs(source, expected):
    assert parser.expand_refs(*source) == expected


def test_reference_expansion():
    ps_noref = parser.Parser(parser_path / "groupBy.json")
    ps_ref = parser.Parser(parser_path / "groupBy-defs.json")
    del ps_ref.spec["adtl"]["defs"]
    assert ps_ref.spec == ps_noref.spec


def test_reference_expansion_with_include():
    ps_noinclude = parser.Parser(parser_path / "groupBy-defs.toml")
    ps_include = parser.Parser(parser_path / "groupBy-defs-include.toml")
    del ps_noinclude.spec["adtl"]["defs"]
    del ps_include.spec["adtl"]["include-def"]
    assert ps_noinclude.spec == ps_include.spec


def test_external_definitions():
    with pytest.raises(KeyError):
        parser.Parser(parser_path / "groupBy-external-defs.toml")
    ps = parser.Parser(
        parser_path / "groupBy-external-defs.toml",
        include_defs=[parser_path / "include-def.toml"],
    )
    assert ps.defs["sexMapping"]["values"] == {
        "1": "male",
        "2": "female",
        "3": "non_binary",
    }


@pytest.mark.parametrize(
    "source",
    [str(parser_path / "apply.toml"), parser_path / "epoch.json"],
    ids=["toml", "json"],
)
def test_read_definition(source):
    assert parser.read_definition(source)


def test_validate_spec_no_header():
    with pytest.raises(ValueError, match="adtl\n  Field required"):
        _ = parser.Parser(dict())


def test_validate_spec_malformed_header():
    with pytest.raises(ValueError, match="adtl.description\n  Field required"):
        _ = parser.Parser({"adtl": {"name": "spec_without_tables"}})


@pytest.mark.parametrize(
    "source,expected",
    [
        (parser_path / "oneToMany.json", ["observation"]),
        (parser_path / "groupBy.json", ["subject"]),
    ],
    ids=["oneToMany", "groupBy"],
)
def test_load_spec(source, expected):
    ps = parser.Parser(source)
    assert list(ps.tables.keys()) == expected


def test_unsupported_spec_format_raises_exception():
    with pytest.raises(ValueError, match="Unsupported file format"):
        parser.read_definition(parser_path / "epoch.yml")
    with pytest.raises(ValueError, match="adtl specification format not supported"):
        parser.Parser(str(parser_path / "epoch.yml"))


@pytest.mark.parametrize(
    "source,error",
    [
        (
            parser_path / "groupBy-missing-kind.json",
            "adtl.tables.subject.kind\n  Field required",
        ),
        (
            parser_path / "groupBy-missing-table.json",
            "Parser specification missing tables: subject",
        ),
        (
            {
                "adtl": {
                    "name": "Bad groupBy",
                    "description": "groupBy with bad aggregation type",
                    "tables": {
                        "subject": {
                            "kind": "groupBy",
                            "groupBy": "subject_id",
                            "aggregation": "foobar",
                        }
                    },
                }
            },
            "adtl.tables.subject.aggregation\n  Input should be 'lastNotNull' or 'applyCombinedType'",
        ),
        (
            {
                "adtl": {
                    "name": "sampleOneToMany",
                    "description": "One to Many example",
                    "tables": {"observation": {"kind": "oneToMany"}},
                }
            },
            "'discriminator' is required for 'oneToMany' tables",
        ),
        (
            {
                "adtl": {
                    "name": "invalid_spec",
                    "description": "No groupby",
                    "tables": {"table-1": {"kind": "groupBy"}},
                }
            },
            "groupBy key is required for 'groupBy' tables",
        ),
        (
            {
                "adtl": {
                    "name": "invalid_spec",
                    "description": "No groupby",
                    "tables": {"table-1": {"kind": "groupBy", "groupBy": "id"}},
                }
            },
            "aggregation is required for 'groupBy' tables",
        ),
        (
            {
                "adtl": {
                    "name": "invalid_spec",
                    "description": "No groupby",
                    "tables": {
                        "table-1": {
                            "kind": "groupBy",
                            "groupBy": "id",
                            "aggregation": "lastNotNull",
                        }
                    },
                },
                "table-1": [],
            },
            "Long format tables must be given kind 'oneToMany'",
        ),
        (
            {
                "adtl": {
                    "name": "invalid_spec",
                    "description": "No groupby",
                    "tables": {"table-1": {"kind": "constant"}},
                },
                "table-1": {},
                "table-2": [],
            },
            "Parser specification has tables not defined in the header: table-2",
        ),
    ],
    ids=[
        "missing-kind",
        "missing-table",
        "incorrect-aggregation",
        "missing-discriminator",
        "missing-groupby",
        "missing-aggregation",
        "wrong-type-tables",
        "extra-tables",
    ],
)
def test_invalid_header_raises_error(source, error):
    with pytest.raises(ValueError, match=error):
        _ = parser.Parser(source)


# Test expand_for

FOR_PATTERN = [
    {
        "name": "history_of_fever",
        "phase": "followup",
        "date": {"field": "flw2_survey_date_{n}"},
        "is_present": {"field": "flw2_fever_{n}", "values": {"0": False, "1": True}},
        "if": {"not": {"flw2_fever_{n}": 2}},
        "for": {"n": {"range": [1, 3]}},
    }
]

FOR_PATTERN_LIST = [
    {
        "name": "headache",
        "phase": "admission",
        "date": {"field": "flw2_survey_date_{n}"},
        "is_present": {
            "combinedType": "any",
            "fields": ["flw2_headache_{n}", "headache"],
            "values": {"0": False, "1": True},
            5: "non-string-key",
        },
        "if": {"not": {"flw2_headache_{n}": 2}},
        "for": {"n": {"range": [1, 2]}},
    }
]

EXPANDED_FOR_PATTERN_LIST = [
    {
        "name": "headache",
        "phase": "admission",
        "date": {"field": "flw2_survey_date_1"},
        "is_present": {
            "combinedType": "any",
            "fields": ["flw2_headache_1", "headache"],
            "values": {"0": False, "1": True},
            5: "non-string-key",
        },
        "if": {"not": {"flw2_headache_1": 2}},
    },
    {
        "name": "headache",
        "phase": "admission",
        "date": {"field": "flw2_survey_date_2"},
        "is_present": {
            "combinedType": "any",
            "fields": ["flw2_headache_2", "headache"],
            "values": {"0": False, "1": True},
            5: "non-string-key",
        },
        "if": {"not": {"flw2_headache_2": 2}},
    },
]

FOR_PATTERN_NOT_DICT = [
    {
        "name": "history_of_fever",
        "phase": "followup",
        "date": {"field": "flw2_survey_date_{n}"},
        "is_present": {"field": "flw2_fever_{n}", "values": {"0": False, "1": True}},
        "for": [1, 3],
    }
]

FOR_PATTERN_BAD_RULE = [
    {
        "name": "history_of_fever",
        "phase": "followup",
        "date": {"field": "flw2_survey_date_{n}"},
        "is_present": {"field": "flw2_fever_{n}", "values": {"0": False, "1": True}},
        "for": {"n": {"includes": [1, 3]}},
    }
]


EXPANDED_FOR_PATTERN = [
    {
        "name": "history_of_fever",
        "phase": "followup",
        "date": {"field": "flw2_survey_date_1"},
        "is_present": {"field": "flw2_fever_1", "values": {"0": False, "1": True}},
        "if": {"not": {"flw2_fever_1": 2}},
    },
    {
        "name": "history_of_fever",
        "phase": "followup",
        "date": {"field": "flw2_survey_date_2"},
        "is_present": {"field": "flw2_fever_2", "values": {"0": False, "1": True}},
        "if": {"not": {"flw2_fever_2": 2}},
    },
    {
        "name": "history_of_fever",
        "phase": "followup",
        "date": {"field": "flw2_survey_date_3"},
        "is_present": {"field": "flw2_fever_3", "values": {"0": False, "1": True}},
        "if": {"not": {"flw2_fever_3": 2}},
    },
]

FOR_PATTERN_ANY = [
    {
        "name": "history_of_fever",
        "phase": "followup",
        "date": {"field": "flw2_survey_date_{n}"},
        "is_present": {"field": "flw2_fever_{n}", "values": {"0": False, "1": True}},
        "if": {"any": [{"flw2_fever_{n}": 0}, {"flw2_fever_{n}": 1}]},
        "for": {"n": {"range": [1, 2]}},
    }
]

EXPANDED_FOR_PATTERN_ANY = [
    {
        "name": "history_of_fever",
        "phase": "followup",
        "date": {"field": "flw2_survey_date_1"},
        "is_present": {"field": "flw2_fever_1", "values": {"0": False, "1": True}},
        "if": {"any": [{"flw2_fever_1": 0}, {"flw2_fever_1": 1}]},
    },
    {
        "name": "history_of_fever",
        "phase": "followup",
        "date": {"field": "flw2_survey_date_2"},
        "is_present": {"field": "flw2_fever_2", "values": {"0": False, "1": True}},
        "if": {"any": [{"flw2_fever_2": 0}, {"flw2_fever_2": 1}]},
    },
]

FOR_PATTERN_MULTI_VAR = [
    {
        "field": "field_{x}_{y}",
        "if": {"field_{x}_{y}": 1},
        "for": {"x": [1, 2], "y": [3, 4]},
    }
]

EXPANDED_FOR_PATTERN_MULTI_VAR = [
    {
        "field": "field_1_3",
        "if": {"field_1_3": 1},
    },
    {
        "field": "field_1_4",
        "if": {"field_1_4": 1},
    },
    {
        "field": "field_2_3",
        "if": {"field_2_3": 1},
    },
    {
        "field": "field_2_4",
        "if": {"field_2_4": 1},
    },
]

FOR_PATTERN_APPLY = [
    {
        "name": "cough",
        "phase": "followup",
        "date": {"field": "flw2_survey_date_{n}"},
        "start_date": {
            "field": "flw2_survey_date_{n}",
            "apply": {
                "function": "startDate",
                "params": [
                    7,
                ],
            },
        },
        "for": {"n": {"range": [1, 2]}},
        "is_present": {
            "field": "flw2_pers_cough_dry_{n}",
            "values": {"0": False, "1": True},
        },
    }
]

EXPANDED_FOR_PATTERN_APPLY = [
    {
        "name": "cough",
        "phase": "followup",
        "date": {"field": "flw2_survey_date_1"},
        "start_date": {
            "field": "flw2_survey_date_1",
            "apply": {
                "function": "startDate",
                "params": [
                    7,
                ],
            },
        },
        "is_present": {
            "field": "flw2_pers_cough_dry_1",
            "values": {"0": False, "1": True},
        },
    },
    {
        "name": "cough",
        "phase": "followup",
        "date": {"field": "flw2_survey_date_2"},
        "start_date": {
            "field": "flw2_survey_date_2",
            "apply": {
                "function": "startDate",
                "params": [
                    7,
                ],
            },
        },
        "is_present": {
            "field": "flw2_pers_cough_dry_2",
            "values": {"0": False, "1": True},
        },
    },
]


@pytest.mark.parametrize(
    "source,expected",
    [
        (FOR_PATTERN, EXPANDED_FOR_PATTERN),
        (FOR_PATTERN_ANY, EXPANDED_FOR_PATTERN_ANY),
        (FOR_PATTERN_MULTI_VAR, EXPANDED_FOR_PATTERN_MULTI_VAR),
        (FOR_PATTERN_APPLY, EXPANDED_FOR_PATTERN_APPLY),
        (FOR_PATTERN_LIST, EXPANDED_FOR_PATTERN_LIST),
    ],
    ids=[
        "simple_for",
        "for_with_any",
        "for_multi_var",
        "for_with_apply",
        "for_with_list",
    ],
)
def test_expand_for(source, expected):
    assert parser.expand_for(source) == expected


def test_expand_for_exceptions():
    with pytest.raises(ValueError, match="is not a dictionary of variables"):
        parser.expand_for(FOR_PATTERN_NOT_DICT)

    with pytest.raises(ValueError, match="can only have lists or ranges"):
        parser.expand_for(FOR_PATTERN_BAD_RULE)
