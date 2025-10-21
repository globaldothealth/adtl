import pytest
from shared import parser_path, sources_path

import adtl.parser as parser

ONE_MANY_SOURCE = [
    {"dt": "2022-02-05", "headache_cmyn": 1, "cough_cmyn": 1, "dyspnea_cmyn": 0}
]

ONE_MANY_OUTPUT = [
    {"date": "2022-02-05", "name": "headache", "is_present": True},
    {"date": "2022-02-05", "name": "cough", "is_present": True},
]


def test_one_to_many():
    actual_one_many_output_rows = list(
        parser.Parser(parser_path / "oneToMany.json")
        .parse_rows(ONE_MANY_SOURCE, "test_one_to_many")
        .read_table("observation")
    )
    actual_one_many_output_csv = list(
        parser.Parser(parser_path / "oneToMany.json")
        .parse(sources_path / "oneToMany.csv")
        .read_table("observation")
    )
    assert actual_one_many_output_rows == ONE_MANY_OUTPUT
    assert actual_one_many_output_csv == ONE_MANY_OUTPUT


ONE_MANY_OUTPUT_COMMON = [
    {"dataset_id": "ONE_TO_MANY", **item} for item in ONE_MANY_OUTPUT
]


def test_one_to_many_with_common_mappings():
    one_many_output_rows = list(
        parser.Parser(parser_path / "oneToMany-commonMappings.json")
        .parse_rows(ONE_MANY_SOURCE, "test_one_to_many_common_mappings")
        .read_table("observation")
    )
    assert one_many_output_rows == ONE_MANY_OUTPUT_COMMON


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


def test_one_to_many_correct_if_behaviour():
    actual_row = list(
        parser.Parser(parser_path / "oneToMany-missingIf.toml")
        .parse(sources_path / "oneToManyIf.csv")
        .read_table("observation")
    )
    actual_row_missing = list(
        parser.Parser(parser_path / "oneToMany-missingIf.toml")
        .parse(sources_path / "oneToManyIf-missing.csv")
        .read_table("observation")
    )

    assert actual_row == ONE_MANY_IF_OUTPUT
    assert actual_row_missing == ONE_MANY_IF_MISSINGDATA_OUTPUT


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


def test_apply_in_one_to_many():
    apply_observations_output = list(
        parser.Parser(parser_path / "apply-observations.toml")
        .parse_rows(APPLY_OBSERVATIONS_SOURCE, "apply_obs")
        .read_table("observation")
    )

    assert apply_observations_output == APPLY_OBSERVATIONS_OUTPUT


# Default 'if' rule data & test

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
        "long_rule_field_option_skip",
        "long_rule_text_skip",
        "long_rule_field_option_value",
        "long_rule_field_option_comb",
        "long_rule_field_option_value_comb",
        "long_rule_ignore_missing_key",
    ],
)
def test_default_if_rule_is_correct(rule, expected):
    psr = parser.Parser(parser_path / "oneToMany-missingIf.toml")
    assert psr._default_if("observation", rule)["if"] == expected
