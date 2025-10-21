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

ONE_MANY_SOURCE = [
    {"dt": "2022-02-05", "headache_cmyn": 1, "cough_cmyn": 1, "dyspnea_cmyn": 0}
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


def test_missing_key_parse_if():
    with pytest.raises(KeyError, match="headache_v2"):
        parser.Parser(TEST_PARSERS_PATH / "oneToMany-missingIf.toml").parse(
            TEST_SOURCES_PATH / "oneToManyIf-missingError.csv"
        )


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
