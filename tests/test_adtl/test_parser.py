from __future__ import annotations

import json
import warnings
from pathlib import Path

import pytest
from pytest_unordered import unordered

import adtl.parser as parser

TEST_PARSERS_PATH = Path(__file__).parent / "parsers"
TEST_SOURCES_PATH = Path(__file__).parent / "sources"
TEST_SCHEMAS_PATH = Path(__file__).parent / "schemas"

# test the header info is being picked up


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


def test_provide_empty_values_definition(snapshot):
    ps = parser.Parser(TEST_PARSERS_PATH / "emptyFields.json")

    transformed_csv_data = ps.parse(TEST_SOURCES_PATH / "emptyFields.csv").data[
        "observation"
    ]

    assert transformed_csv_data == snapshot


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


# Check writing out and validation works as expected


def test_parse_write_buffer(snapshot):
    groupby_source = [
        {
            "sex": "1",
            "subjid": "S007",
            "dsstdat": "2020-05-06",
            "hostdat": "2020-06-08",
        },
        {
            "sex": "2",
            "subjid": "S001",
            "dsstdat": "2022-01-11",
            "hostdat": "2020-06-08",
        },
    ]
    ps = parser.Parser(TEST_PARSERS_PATH / "groupBy.json")
    buf = ps.parse_rows(groupby_source, "test_groupby").write_csv("subject")
    assert buf == snapshot


def test_validation(snapshot):
    invalid_groupby_source = [
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

    ps = parser.Parser(TEST_PARSERS_PATH / "groupBy-with-schema.json")
    buf = ps.parse_rows(invalid_groupby_source, "test_groupby_invalid").write_csv(
        "subject"
    )
    assert buf == snapshot


# check static tables


def test_constant_table():
    ps = parser.Parser(TEST_PARSERS_PATH / "constant.json").parse_rows([{"x": 1}], "x")
    assert list(ps.read_table("metadata")) == [
        {"dataset": "constant", "version": "20220505.1", "format": "csv"}
    ]


# kind=groupby behaviour


def test_multi_id_groupby(snapshot):
    multi_id_groupby = [
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

    ps = parser.Parser(TEST_PARSERS_PATH / "groupBy-multi-id.json")
    buf = ps.parse_rows(multi_id_groupby, "groupby_multi_id").write_csv("subject")
    assert buf == snapshot


def test_apply_when_values_are_present_groupby():
    data = [
        {
            "subjid": "S007",
            "brthdtc": "1996-02-24",
            "dsstdat": "2023-02-24",
            "age": "22",
            "ageu": 1,
            "icu_hostdat": 1,
        }
    ]

    apply_values_present_output = list(
        parser.Parser(TEST_PARSERS_PATH / "apply.toml")
        .parse_rows(data, "apply")
        .read_table("subject")
    )

    assert apply_values_present_output == [
        {
            "subject_id": "S007",
            "age": pytest.approx(27.0, 0.001),
            "icu_admitted": True,
            "dob_year": 1974,
        }
    ]


def test_apply_when_values_not_present_groupby():
    data = [
        {
            "subjid": "S007",
            "brthdtc": "",
            "dsstdat": "2023-02-24",
            "age": "22",
            "ageu": 1,
            "icu_hostdat": "",
        }
    ]
    apply_values_absent_output = list(
        parser.Parser(TEST_PARSERS_PATH / "apply.toml")
        .parse_rows(data, "apply_absent")
        .read_table("subject")
    )

    assert apply_values_absent_output == [
        {"subject_id": "S007", "age": 22.0, "icu_admitted": False, "dob_year": 2001}
    ]


def test_no_overwriting_groupby():
    prsr = parser.Parser(TEST_PARSERS_PATH / "stop-overwriting.toml")

    overwriting_output = list(
        prsr.parse(TEST_SOURCES_PATH / "stop-overwriting.csv").read_table("visit")
    )
    # Can't use snapshot as the antiviral order changes between runs
    assert overwriting_output == [
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
            "treatment_antiviral_type": unordered(
                ["Ribavirin", "Lopinavir", "Interferon"]
            ),
        },
    ]


@pytest.mark.parametrize(
    "verbosity,expected_warnings",
    [
        (False, None),
        (True, "Multiple rows of data found for"),
    ],
)
def test_overwriting_with_strict_groupby(verbosity, expected_warnings):
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

    assert overwritten_output == [
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


# General tests


def test_parser_clear_removes_all_data():
    ps = parser.Parser(TEST_PARSERS_PATH / "oneToMany.json")
    ps.data = {"observation": []}
    ps.clear()
    assert ps.data == {}


def test_read_table_raises_error_if_wrong_table_name():
    source = [
        {
            "Entry_ID": 1,
            "Epoch": "11/01/1999",
            "SomeDate": "24/01/1999",
            "Text": "Lorem Ipsum",
        }
    ]
    with pytest.raises(ValueError, match="Invalid table name"):
        list(
            parser.Parser(TEST_PARSERS_PATH / "epoch.json")
            .parse_rows(source, "one_to_many")
            .read_table("wrong-name")
        )


@pytest.mark.filterwarnings("ignore:No matches found")
@pytest.mark.filterwarnings("ignore:Could not construct date")
def test_return_unmapped(snapshot):
    transformed_csv_data = (
        parser.Parser(TEST_PARSERS_PATH / "return-unmapped.toml")
        .parse(TEST_SOURCES_PATH / "return-unmapped.csv")
        .write_csv("subject")
    )
    assert transformed_csv_data == snapshot


def test_subschema_validation_for_large_schemas(snapshot):
    transformed_csv_data = (
        parser.Parser(TEST_PARSERS_PATH / "long-oneof-parser.toml")
        .parse(TEST_SOURCES_PATH / "long-oneof.csv")
        .data["long"]
    )
    assert transformed_csv_data == snapshot
