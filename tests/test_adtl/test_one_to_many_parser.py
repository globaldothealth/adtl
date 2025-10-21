from pathlib import Path

import adtl.parser as parser

TEST_PARSERS_PATH = Path(__file__).parent / "parsers"
TEST_SOURCES_PATH = Path(__file__).parent / "sources"

ONE_MANY_SOURCE = [
    {"dt": "2022-02-05", "headache_cmyn": 1, "cough_cmyn": 1, "dyspnea_cmyn": 0}
]

ONE_MANY_OUTPUT = [
    {"date": "2022-02-05", "name": "headache", "is_present": True},
    {"date": "2022-02-05", "name": "cough", "is_present": True},
]


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


ONE_MANY_OUTPUT_COMMON = [
    {"dataset_id": "ONE_TO_MANY", **item} for item in ONE_MANY_OUTPUT
]


def test_one_to_many_with_common_mappings():
    one_many_output_rows = list(
        parser.Parser(TEST_PARSERS_PATH / "oneToMany-commonMappings.json")
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
        parser.Parser(TEST_PARSERS_PATH / "apply-observations.toml")
        .parse_rows(APPLY_OBSERVATIONS_SOURCE, "apply_obs")
        .read_table("observation")
    )

    assert apply_observations_output == APPLY_OBSERVATIONS_OUTPUT
