from __future__ import annotations

import collections
import contextlib
import io
import json
import warnings
from pathlib import Path

import pytest
from pytest_unordered import unordered

import adtl.parser as parser

TEST_PARSERS_PATH = Path(__file__).parent / "parsers"
TEST_SOURCES_PATH = Path(__file__).parent / "sources"
TEST_SCHEMAS_PATH = Path(__file__).parent / "schemas"

LIVER_DISEASE = [
    {
        "field": "modliv",
        "values": {"1": True, "0": False, "2": None},
        "description": "Moderate liver disease",
    },
    {
        "field": "mildliver",
        "values": {"1": True, "0": False, "2": None},
        "description": "Mild liver disease",
    },
]

ONE_MANY_SOURCE = [
    {"dt": "2022-02-05", "headache_cmyn": 1, "cough_cmyn": 1, "dyspnea_cmyn": 0}
]

ONE_MANY_OUTPUT = [
    {"date": "2022-02-05", "name": "headache", "is_present": True},
    {"date": "2022-02-05", "name": "cough", "is_present": True},
]

ONE_MANY_OUTPUT_COMMON = [
    {"dataset_id": "ONE_TO_MANY", **item} for item in ONE_MANY_OUTPUT
]

ONE_MANY_IF_OUTPUT = [
    {
        "date": "2022-02-05",
        "name": "headache",
        "phase": "admission",
        "is_present": False,
        "adtl_valid": True,
    },
    {
        "date": "2022-02-05",
        "name": "oxygen_saturation",
        "phase": "admission",
        "value": 87.0,
        "adtl_valid": True,
    },
    {
        "date": "2022-02-05",
        "name": "cough",
        "phase": "admission",
        "is_present": True,
        "adtl_valid": True,
    },
    {
        "date": "2022-02-05",
        "name": "pao2_sample_type",
        "phase": "study",
        "text": "Capillary",
        "adtl_valid": True,
    },
    {
        "date": "2022-02-06",
        "name": "history_of_fever",
        "phase": "followup",
        "is_present": True,
        "adtl_valid": True,
    },
    {
        "date": "2022-02-07",
        "name": "history_of_fever",
        "phase": "followup",
        "is_present": False,
        "adtl_valid": True,
    },
    {
        "date": "2022-02-05",
        "name": "fatigue_malaise",
        "phase": "followup",
        "is_present": True,
        "adtl_valid": True,
    },
    {
        "date": "2022-02-05",
        "name": "severe_dehydration",
        "phase": "admission",
        "is_present": False,
        "adtl_valid": True,
    },
]

ONE_MANY_IF_MISSINGDATA_OUTPUT = [
    {
        "date": "2022-02-05",
        "name": "cough",
        "phase": "admission",
        "is_present": True,
        "adtl_valid": True,
    },
    {
        "date": "2022-02-07",
        "name": "history_of_fever",
        "phase": "followup",
        "is_present": False,
        "adtl_valid": True,
    },
]

SOURCE_GROUPBY = [
    {"sex": "1", "subjid": "S007", "dsstdat": "2020-05-06", "hostdat": "2020-06-08"},
    {"sex": "2", "subjid": "S001", "dsstdat": "2022-01-11", "hostdat": "2020-06-08"},
]
# Checks ID mapping from multiple fields
SOURCE_GROUPBY_MULTI_ID = [
    {
        "sex": "1",
        "subjid": "",
        "othid": "P007",
        "dsstdat": "2020-05-06",
        "hostdat": "2020-06-08",
    },
    {
        "sex": "2",
        "subjid": "S001",
        "othid": "P008",
        "dsstdat": "2022-01-11",
        "hostdat": "2020-06-08",
    },
]

SOURCE_GROUPBY_INVALID = [
    {
        "sex": "1",
        "subjid": "S007",
        "dsstdat": "2020-05-06",
        "hostdat": "2020-06-08",
        "ethnic": "1",
    },
    {
        "sex": "",
        "subjid": "S007",
        "dsstdat": "",
        "hostdat": "",
        "ethnic": "",
    },
    {
        "sex": "5",
        "subjid": "S001",
        "dsstdat": "2022-01-11",
        "hostdat": "8/6/2022",
        "ethnic": "2",
    },
    {
        "sex": "1",
        "subjid": "S009",
        "dsstdat": "2020-05-06",
        "hostdat": "8/6/2020",
        "ethnic": "3",
    },
]

BUFFER_GROUPBY = """
sex_at_birth,subject_id,dataset_id,country_iso3,enrolment_date,admission_date
male,S007,dataset-2020-03-23,GBR,2020-05-06,2020-06-08
female,S001,dataset-2020-03-23,GBR,2022-01-11,2020-06-08
"""

SOURCE_APPLY_PRESENT = [
    {
        "subjid": "S007",
        "brthdtc": "1996-02-24",
        "dsstdat": "2023-02-24",
        "age": "22",
        "ageu": 1,
        "icu_hostdat": 1,
    }
]
APPLY_PRESENT_OUTPUT = [
    {
        "subject_id": "S007",
        "age": pytest.approx(27.0, 0.001),
        "icu_admitted": True,
        "dob_year": 1974,
    }
]
SOURCE_APPLY_ABSENT = [
    {
        "subjid": "S007",
        "brthdtc": "",
        "dsstdat": "2023-02-24",
        "age": "22",
        "ageu": 1,
        "icu_hostdat": "",
    }
]
APPLY_ABSENT_OUTPUT = [
    {"subject_id": "S007", "age": 22.0, "icu_admitted": False, "dob_year": 2001}
]

APPLY_OBSERVATIONS_SOURCE = [
    {
        "dsstdat": "2023-02-01",
        "flw_headache": "1",
        "flw_cough": "1",
        "dyspnea_cmyn": "0",
    }
]

APPLY_OBSERVATIONS_OUTPUT = [
    {
        "date": "2023-02-01",
        "start_date": "2023-01-22",
        "phase": "followup",
        "duration_type": "event",
        "name": "headache",
        "is_present": True,
    },
    {
        "date": "2023-02-01",
        "start_date": "2023-01-25",
        "phase": "followup",
        "duration_type": "event",
        "name": "cough",
        "is_present": True,
    },
]

RULE_FIELD_OPTION_SKIP = {
    "field": "aidshiv_mhyn",
    "values": {"1": True, "0": False},
    "can_skip": True,
}

OBSERVATION_RULE_FIELD_OPTION_SKIP = {
    "name": "bleeding",
    "phase": "admission",
    "date": "2023-05-18",
    "is_present": {
        "field": "bleed_ceterm_v2",
        "values": {"1": True, "0": False},
        "can_skip": True,
    },
}
OBSERVATION_RULE_TEXT_SKIP = {
    "name": "temperature_celsius",
    "phase": "admission",
    "date": "2023-05-18",
    "value": {
        "field": "temperature_adm",
        "can_skip": True,
    },
}
OBSERVATION_RULE_FIELD_OPTION_VALUE = {
    "name": "temperature_celsius",
    "phase": "admission",
    "date": "2023-05-22",
    "value": {
        "field": "temp_vsorres",
        "source_unit": {"field": "temp_vsorresu", "values": {"1": "°C", "2": "°F"}},
    },
}

OBSERVATION_RULE_FIELD_OPTION_COMB = {
    "name": "cough",
    "phase": "admission",
    "date": "2023-05-22",
    "is_present": {
        "combinedType": "any",
        "fields": [
            {"field": "cough_ceoccur_v2", "values": {"1": "true", "0": "false"}},
            {
                "field": "coughsput_ceoccur_v2",
                "values": {"1": "true", "0": "false"},
                "can_skip": "true",
            },
            {
                "field": "coughhb_ceoccur_v2",
                "values": {"1": "true", "0": "false"},
                "can_skip": "true",
            },
        ],
    },
}

OBSERVATION_RULE_FIELD_OPTION_VALUE_COMB = {
    "name": "temperature_celsius",
    "phase": "study",
    "date": "2023-05-27",
    "value": {
        "combinedType": "max",
        "fields": [
            {
                "field": "temp_v1",
            },
            {
                "field": "temp_v2",
                "can_skip": "true",
            },
        ],
    },
}


def test_one_to_many():
    actual_one_many_output_rows = list(
        parser.Parser(TEST_PARSERS_PATH / "oneToMany.json")
        .parse_rows(ONE_MANY_SOURCE, "test_one_to_many")
        .read_table("observation")
    )
    actual_one_many_output_csv = list(
        parser.Parser(TEST_PARSERS_PATH / "oneToMany.json")
        .parse(TEST_SOURCES_PATH / "oneToMany.csv")
        .read_table("observation")
    )
    assert actual_one_many_output_rows == ONE_MANY_OUTPUT
    assert actual_one_many_output_csv == ONE_MANY_OUTPUT


def test_one_to_many_with_common_mappings():
    one_many_output_rows = list(
        parser.Parser(TEST_PARSERS_PATH / "oneToMany-commonMappings.json")
        .parse_rows(ONE_MANY_SOURCE, "test_one_to_many_common_mappings")
        .read_table("observation")
    )
    assert one_many_output_rows == ONE_MANY_OUTPUT_COMMON


@pytest.mark.parametrize(
    "rule,expected",
    [
        (
            OBSERVATION_RULE_FIELD_OPTION_SKIP,
            {
                "any": [
                    {"bleed_ceterm_v2": "1", "can_skip": True},
                    {"bleed_ceterm_v2": "0", "can_skip": True},
                ]
            },
        ),
        (OBSERVATION_RULE_TEXT_SKIP, {"temperature_adm": {"!=": ""}, "can_skip": True}),
        (OBSERVATION_RULE_FIELD_OPTION_VALUE, {"temp_vsorres": {"!=": ""}}),
        (
            OBSERVATION_RULE_FIELD_OPTION_COMB,
            {
                "any": [
                    {"cough_ceoccur_v2": "1"},
                    {"cough_ceoccur_v2": "0"},
                    {"coughsput_ceoccur_v2": "1", "can_skip": True},
                    {"coughsput_ceoccur_v2": "0", "can_skip": True},
                    {"coughhb_ceoccur_v2": "1", "can_skip": True},
                    {"coughhb_ceoccur_v2": "0", "can_skip": True},
                ]
            },
        ),
        (
            OBSERVATION_RULE_FIELD_OPTION_VALUE_COMB,
            {
                "any": [
                    {"temp_v1": {"!=": ""}},
                    {"temp_v2": {"!=": ""}, "can_skip": True},
                ]
            },
        ),
        (
            {
                "name": "demog_country",
                "phase": "presentation",
                "value": {
                    "field": "slider_country",
                    "ignoreMissingKey": True,
                    "values": {
                        "Russian Federation": "Russia",
                        "Gambia": "Gambia The",
                    },
                },
            },
            {"slider_country": {"!=": ""}},
        ),
    ],
    ids=[
        "observation_rule_field_option_skip",
        "observation_rule_text_skip",
        "observation_rule_field_option_value",
        "observation_rule_field_option_comb",
        "observation_rule_field_option_value_comb",
        "long_rule_ignore_missing_key",
    ],
)
def test_default_if_rule_is_correct(rule, expected):
    psr = parser.Parser(TEST_PARSERS_PATH / "oneToMany-missingIf.toml")
    assert psr._default_if("observation", rule)["if"] == expected


def test_one_to_many_correct_if_behaviour():
    actual_row = list(
        parser.Parser(TEST_PARSERS_PATH / "oneToMany-missingIf.toml")
        .parse(TEST_SOURCES_PATH / "oneToManyIf.csv")
        .read_table("observation")
    )
    actual_row_missing = list(
        parser.Parser(TEST_PARSERS_PATH / "oneToMany-missingIf.toml")
        .parse(TEST_SOURCES_PATH / "oneToManyIf-missing.csv")
        .read_table("observation")
    )

    assert actual_row == ONE_MANY_IF_OUTPUT
    assert actual_row_missing == ONE_MANY_IF_MISSINGDATA_OUTPUT


# test exceptions


def test_invalid_operand_parse_if():
    with pytest.raises(ValueError, match="Unrecognized operand"):
        parser.parse_if(
            {"outcome_type": 1, "outcome_date": "2022-06-04"},
            {"outcome_type": {"<>": 5}},
        )
    with pytest.raises(ValueError, match="if-subexpressions should be a dictionary"):
        parser.parse_if(
            {"outcome_type": 1, "outcome_date": "2022-06-04"},
            {"outcome_type": {"<>", 5}},
        )


def test_missing_key_parse_if():
    with pytest.raises(KeyError, match="headache_v2"):
        parser.Parser(TEST_PARSERS_PATH / "oneToMany-missingIf.toml").parse(
            TEST_SOURCES_PATH / "oneToManyIf-missingError.csv"
        )


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


def test_parse_write_buffer(snapshot):
    ps = parser.Parser(TEST_PARSERS_PATH / "groupBy.json")
    buf = ps.parse_rows(SOURCE_GROUPBY, "test_groupby").write_csv("subject")
    assert buf == snapshot


def test_validation(snapshot):
    ps = parser.Parser(TEST_PARSERS_PATH / "groupBy-with-schema.json")
    buf = ps.parse_rows(SOURCE_GROUPBY_INVALID, "test_groupby_invalid").write_csv(
        "subject"
    )
    assert buf == snapshot


def test_multi_id_groupby(snapshot):
    ps = parser.Parser(TEST_PARSERS_PATH / "groupBy-multi-id.json")
    buf = ps.parse_rows(SOURCE_GROUPBY_MULTI_ID, "groupby_multi_id").write_csv(
        "subject"
    )
    assert buf == snapshot


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


def test_parser_clear():
    ps = parser.Parser(TEST_PARSERS_PATH / "oneToMany.json")
    ps.data = {"observation": []}
    ps.clear()
    assert ps.data == {}


def test_read_table_raises_error():
    with pytest.raises(ValueError, match="Invalid table"):
        list(
            parser.Parser(TEST_PARSERS_PATH / "oneToMany.json")
            .parse_rows(ONE_MANY_SOURCE, "one_to_many")
            .read_table("obs")
        )


def test_constant_table():
    ps = parser.Parser(TEST_PARSERS_PATH / "constant.json").parse_rows([{"x": 1}], "x")
    assert list(ps.read_table("metadata")) == [
        {"dataset": "constant", "version": "20220505.1", "format": "csv"}
    ]


def test_get_date_fields():
    with (Path(__file__).parent / "parsers" / "test.schema.json").open() as fp:
        schema = json.load(fp)
        assert parser.get_date_fields(schema) == unordered(
            ["enrolment_date", "admission_date"]
        )


def test_default_date_format(snapshot):
    transformed_csv_data = (
        parser.Parser(TEST_PARSERS_PATH / "epoch.json")
        .parse(TEST_SOURCES_PATH / "epoch.csv")
        .write_csv("table")
    )
    assert transformed_csv_data == snapshot


def test_make_fields_optional():
    with (TEST_SCHEMAS_PATH / "epoch-oneOf.schema.json").open() as fp:
        schema = json.load(fp)
    assert schema["required"] == ["epoch", "id", "text"]
    assert parser.make_fields_optional(schema, ["text"])["required"] == ["epoch", "id"]
    assert parser.make_fields_optional(schema, ["field_not_present"])["required"] == [
        "epoch",
        "id",
        "text",
    ]
    assert parser.make_fields_optional(schema, ["sex"])["oneOf"] == [
        {"required": []},
        {"required": ["sex_at_birth"]},
    ]
    assert "oneOf" not in parser.make_fields_optional(schema, ["sex", "sex_at_birth"])

    assert schema["anyOf"] == [
        {"required": ["sex", "epoch"]},
        {"required": ["sex_at_birth", "epoch"]},
    ]

    assert parser.make_fields_optional(schema, ["epoch"])["anyOf"] == [
        {"required": ["sex"]},
        {"required": ["sex_at_birth"]},
    ]
    assert parser.make_fields_optional(schema, ["sex", "sex_at_birth"])["anyOf"] == [
        {"required": ["epoch"]}
    ]


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
)
def test_expand_for(source, expected):
    assert parser.expand_for(source) == expected


def test_expand_for_exceptions():
    with pytest.raises(ValueError, match="is not a dictionary of variables"):
        parser.expand_for(FOR_PATTERN_NOT_DICT)

    with pytest.raises(ValueError, match="can only have lists or ranges"):
        parser.expand_for(FOR_PATTERN_BAD_RULE)


# write functions to check that apply is working properly
def test_apply_when_values_are_present():
    apply_values_present_output = list(
        parser.Parser(TEST_PARSERS_PATH / "apply.toml")
        .parse_rows(SOURCE_APPLY_PRESENT, "apply")
        .read_table("subject")
    )

    assert apply_values_present_output == APPLY_PRESENT_OUTPUT


def test_show_report(snapshot):
    ps = parser.Parser(TEST_PARSERS_PATH / "epoch.json")
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


def test_apply_when_values_not_present():
    apply_values_absent_output = list(
        parser.Parser(TEST_PARSERS_PATH / "apply.toml")
        .parse_rows(SOURCE_APPLY_ABSENT, "apply_absent")
        .read_table("subject")
    )

    assert apply_values_absent_output == APPLY_ABSENT_OUTPUT


def test_apply_in_observations_table():
    apply_observations_output = list(
        parser.Parser(TEST_PARSERS_PATH / "apply-observations.toml")
        .parse_rows(APPLY_OBSERVATIONS_SOURCE, "apply_obs")
        .read_table("observation")
    )

    assert apply_observations_output == APPLY_OBSERVATIONS_OUTPUT


def test_skip_field_pattern_present(snapshot):
    transformed_csv_data = (
        parser.Parser(TEST_PARSERS_PATH / "skip_field.json")
        .parse(TEST_SOURCES_PATH / "skip_field_present.csv")
        .write_csv("table")
    )
    assert transformed_csv_data == snapshot


def test_skip_field_pattern_absent(snapshot):
    transformed_csv_data = (
        parser.Parser(TEST_PARSERS_PATH / "skip_field.json")
        .parse(TEST_SOURCES_PATH / "skip_field_absent.csv")
        .write_csv("table")
    )
    assert transformed_csv_data == snapshot


OVERWRITE_OUTPUT = [
    {
        "subject_id": 1,
        "earliest_admission": "2023-11-19",
        "start_date": "2023-11-20",
        "treatment_antiviral_type": unordered(["Ribavirin", "Interferon"]),
    },
    {
        "subject_id": 2,
        "start_date": "2022-11-23",
        "icu_admission_date": unordered(["2020-11-25", "2020-11-30"]),
        "treatment_antiviral_type": ["Lopinavir"],
    },
    {
        "subject_id": 3,
        "start_date": "2020-02-20",
        "treatment_antiviral_type": unordered(["Ribavirin", "Lopinavir", "Interferon"]),
    },
]

OVERWRITTEN_OUTPUT = [
    {
        "subject_id": 1,
        "earliest_admission": "2023-11-19",
        "start_date": "2023-11-19",
        "treatment_antiviral_type": unordered(["Ribavirin"]),
    },
    {
        "subject_id": 2,
        "start_date": "2020-11-23",
        "icu_admission_date": unordered(["2020-11-30"]),
        "treatment_antiviral_type": ["Lopinavir"],
    },
    {
        "subject_id": 3,
        "start_date": "2020-02-20",
        "treatment_antiviral_type": unordered(["Ribavirin"]),
    },
]


def test_no_overwriting():
    prsr = parser.Parser(TEST_PARSERS_PATH / "stop-overwriting.toml")

    overwriting_output = list(
        prsr.parse(TEST_SOURCES_PATH / "stop-overwriting.csv").read_table("visit")
    )
    assert overwriting_output == OVERWRITE_OUTPUT


@pytest.mark.parametrize(
    "verbosity,expected_warnings",
    [
        (False, None),
        (True, "Multiple rows of data found for"),
    ],
)
def test_overwriting_with_strict(verbosity, expected_warnings):
    prsr = parser.Parser(TEST_PARSERS_PATH / "stop-overwriting.toml", verbose=verbosity)
    prsr.tables["visit"]["aggregation"] = "lastNotNullStrict"

    if verbosity:
        with pytest.warns(UserWarning, match=expected_warnings):
            overwritten_output = list(
                prsr.parse(TEST_SOURCES_PATH / "stop-overwriting.csv").read_table(
                    "visit"
                )
            )
    else:
        warnings.filterwarnings("error")  # Treat warnings as errors
        overwritten_output = list(
            prsr.parse(TEST_SOURCES_PATH / "stop-overwriting.csv").read_table("visit")
        )
    assert overwritten_output == OVERWRITTEN_OUTPUT


@pytest.mark.filterwarnings("ignore:No matches found")
@pytest.mark.filterwarnings("ignore:Could not construct date")
def test_return_unmapped(snapshot):
    transformed_csv_data = (
        parser.Parser(TEST_PARSERS_PATH / "return-unmapped.toml")
        .parse(TEST_SOURCES_PATH / "return-unmapped.csv")
        .write_csv("subject")
    )
    assert transformed_csv_data == snapshot


def test_subschema_validation(snapshot):
    transformed_csv_data = (
        parser.Parser(TEST_PARSERS_PATH / "long-oneof-parser.toml")
        .parse(TEST_SOURCES_PATH / "long-oneof.csv")
        .data["long"]
    )
    assert transformed_csv_data == snapshot


def test_different_empty_values(snapshot):
    ps = parser.Parser(TEST_PARSERS_PATH / "oneToMany-emptyFields.json")

    transformed_csv_data = ps.parse(
        TEST_SOURCES_PATH / "oneToMany-emptyFields.csv"
    ).data["observation"]

    assert transformed_csv_data == snapshot
