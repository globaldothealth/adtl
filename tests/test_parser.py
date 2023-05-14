import json
from pathlib import Path
import pytest
from pytest_unordered import unordered

import adtl as parser

RULE_SINGLE_FIELD = {"field": "diabetes_mhyn"}
RULE_SINGLE_FIELD_WITH_MAPPING = {
    "field": "diabetes_mhyn",
    "values": {"1": True, "2": False, "3": None},
}

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

ANTIVIRAL_TYPE = [
    {"field": "antiviral_cmtrt___1", "values": {"1": "Ribavirin"}},
    {"field": "antiviral_cmtrt___2", "values": {"1": "Lopinavir/Ritonvir"}},
    {"field": "antiviral_cmtrt___3", "values": {"1": "Interferon alpha"}},
    {"field": "daily_antiviral_cmtrt___1", "values": {"1": "Ribavirin"}},
    {"field": "daily_antiviral_cmtrt___2", "values": {"1": "Lopinavir/Ritonvir"}},
    {"field": "daily_antiviral_cmtrt___3", "values": {"1": "Interferon alpha"}},
    {"field": "overall_antiviral_cmtrt___1", "values": {"1": "Ribavirin"}},
    {"field": "overall_antiviral_cmtrt___2", "values": {"1": "Lopinavir/Ritonvir"}},
    {"field": "overall_antiviral_cmtrt___3", "values": {"1": "Interferon alpha"}},
]

ROW_CONCISE = {"mildliv": 0, "modliv": 2}
RULE_EXCLUDE = {
    "combinedType": "list",
    "fields": [{"field": "mildliv"}, {"field": "modliv"}],
}

ROW_CONDITIONAL = {"outcome_date": "2022-01-01", "outcome_type": 4}
RULE_CONDITIONAL_OK = {"field": "outcome_date", "if": {"outcome_type": 4}}
RULE_CONDITIONAL_FAIL = {"field": "outcome_date", "if": {"outcome_type": {"<": 4}}}

ROW_UNIT_MONTH = {"age": 18, "age_unit": "1"}
ROW_UNIT_YEAR = {"age": 18, "age_unit": "2"}
RULE_UNIT = {
    "field": "age",
    "unit": "years",
    "source_unit": {"field": "age_unit", "values": {"1": "months", "2": "years"}},
}

RULE_COMBINED_TYPE_ANY = {"combinedType": "any", "fields": LIVER_DISEASE}
RULE_COMBINED_TYPE_ALL = {"combinedType": "all", "fields": LIVER_DISEASE}
RULE_COMBINED_FIRST_NON_NULL = {
    "combinedType": "firstNonNull",
    "fields": [{"field": "first"}, {"field": "second"}],
}
RULE_COMBINED_TYPE_LIST = {"combinedType": "list", "fields": LIVER_DISEASE}
RULE_COMBINED_TYPE_LIST_PATTERN = {
    "combinedType": "list",
    "fields": [
        {"fieldPattern": ".*liv.*", "values": {"1": True, "0": False, "2": None}}
    ],
}

RULE_NON_SENSITIVE = {"field": "id"}
RULE_SENSITIVE = {"field": "id", "sensitive": True}

RULE_DATE_MDY = {"field": "outcome_date", "source_date": "%d/%m/%Y", "date": "%m/%d/%Y"}
RULE_DATE_ISO = {"field": "outcome_date", "source_date": "%d/%m/%Y"}

RULE_COMBINED_TYPE_SET = {
    "combinedType": "set",
    "excludeWhen": "none",
    "fields": ANTIVIRAL_TYPE,
}

ONE_MANY_SOURCE = [
    {"dt": "2022-02-05", "headache_cmyn": 1, "cough_cmyn": 1, "dyspnea_cmyn": 0}
]

ONE_MANY_OUTPUT = [
    {"date": "2022-02-05", "name": "headache", "is_present": True},
    {"date": "2022-02-05", "name": "cough", "is_present": True},
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
    {"subject_id": "S007", "age": pytest.approx(27.0, 0.001), "icu_admitted": True}
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
APPLY_ABSENT_OUTPUT = [{"subject_id": "S007", "age": 22.0, "icu_admitted": False}]

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


@pytest.mark.parametrize(
    "row_rule,expected",
    [
        (({"diabetes_mhyn": "1"}, RULE_SINGLE_FIELD_WITH_MAPPING), True),
        (({"diabetes_mhyn": "1"}, RULE_SINGLE_FIELD), 1),
        (({}, "CONST"), "CONST"),
        (({"modliv": "1", "mildliver": "0"}, RULE_COMBINED_TYPE_ANY), True),
        (({"modliv": "", "mildliver": ""}, RULE_COMBINED_TYPE_ANY), None),
        (({"modliv": "1", "mildliver": "0"}, RULE_COMBINED_TYPE_ALL), False),
        (({"modliv": "1", "mildliver": "0"}, RULE_COMBINED_TYPE_LIST), [True, False]),
        (
            ({"modliv": "1", "mildliver": "0"}, RULE_COMBINED_TYPE_LIST_PATTERN),
            [True, False],
        ),
        (
            (
                {"modliv": "1", "mildliver": "3"},
                {**RULE_COMBINED_TYPE_LIST_PATTERN, "excludeWhen": "none"},
            ),
            [True],
        ),
        (
            ({"modliv": "1", "mildliver": "3"}, RULE_COMBINED_TYPE_LIST_PATTERN),
            [True, None],
        ),
        (({"id": "1"}, RULE_NON_SENSITIVE), 1),
        (
            ({"id": "1"}, RULE_SENSITIVE),
            "6b86b273ff34fce19d6b804eff5a3f5747ada4eaa22f1d49c01e52ddb7875b4b",
        ),
        (
            ({"id": 1}, RULE_SENSITIVE),
            "6b86b273ff34fce19d6b804eff5a3f5747ada4eaa22f1d49c01e52ddb7875b4b",
        ),
        (({"first": "1", "second": ""}, RULE_COMBINED_FIRST_NON_NULL), 1),
        (({"first": "1", "second": "2"}, RULE_COMBINED_FIRST_NON_NULL), 1),
        (({"first": "2", "second": "1"}, RULE_COMBINED_FIRST_NON_NULL), 2),
        (({"first": "", "second": "3"}, RULE_COMBINED_FIRST_NON_NULL), 3),
        (({"first": False, "second": True}, RULE_COMBINED_FIRST_NON_NULL), False),
        (({"first": "", "second": False}, RULE_COMBINED_FIRST_NON_NULL), False),
        ((ROW_CONDITIONAL, RULE_CONDITIONAL_OK), "2022-01-01"),
        ((ROW_CONDITIONAL, RULE_CONDITIONAL_FAIL), None),
        ((ROW_UNIT_MONTH, RULE_UNIT), 1.5),
        ((ROW_UNIT_YEAR, RULE_UNIT), 18),
        (({"outcome_date": "02/05/2022"}, RULE_DATE_MDY), "05/02/2022"),
        (({"outcome_date": "02/05/2022"}, RULE_DATE_ISO), "2022-05-02"),
        (
            (
                {
                    "antiviral_cmtrt___1": "0",
                    "antiviral_cmtrt___2": "1",
                    "antiviral_cmtrt___3": "0",
                    "daily_antiviral_cmtrt___1": "0",
                    "daily_antiviral_cmtrt___2": "1",
                    "daily_antiviral_cmtrt___3": "1",
                    "overall_antiviral_cmtrt___1": "0",
                    "overall_antiviral_cmtrt___2": "0",
                    "overall_antiviral_cmtrt___3": "1",
                },
                RULE_COMBINED_TYPE_SET,
            ),
            unordered(["Lopinavir/Ritonvir", "Interferon alpha"]),
        ),
        (({"first": "", "second": ""}, RULE_COMBINED_FIRST_NON_NULL), None),
    ],
)
def test_get_value(row_rule, expected):
    row, rule = row_rule
    assert parser.get_value(row, rule) == expected


@pytest.mark.parametrize(
    "row_rule,expected",
    [
        ((ROW_CONDITIONAL, {"outcome_type": 4}), True),
        ((ROW_CONDITIONAL, {"not": {"outcome_type": 4}}), False),
        ((ROW_CONDITIONAL, {"outcome_type": {"==": 4}}), True),
        ((ROW_CONDITIONAL, {"outcome_type": 3}), False),
        ((ROW_CONDITIONAL, {"outcome_type": {">": 2}}), True),
        ((ROW_CONDITIONAL, {"outcome_type": {"<": 10}}), True),
        ((ROW_CONDITIONAL, {"outcome_type": {"<=": 4}}), True),
        ((ROW_CONDITIONAL, {"outcome_type": {">=": 4}}), True),
        ((ROW_CONDITIONAL, {"outcome_type": {"<": 10}}), True),
        ((ROW_CONDITIONAL, {"outcome_type": {"!=": 4}}), False),
        (
            (
                ROW_CONDITIONAL,
                {"any": [{"outcome_type": {">": 2}}, {"outcome_date": {"<": "2022"}}]},
            ),
            True,
        ),
        (
            (
                ROW_CONDITIONAL,
                {"all": [{"outcome_type": {">": 2}}, {"outcome_date": {"<": "2022"}}]},
            ),
            False,
        ),
    ],
)
def test_parse_if(row_rule, expected):
    assert parser.parse_if(*row_rule) == expected


def test_one_to_many():
    actual_one_many_output_rows = list(
        parser.Parser(TEST_PARSERS_PATH / "oneToMany.json")
        .parse_rows(ONE_MANY_SOURCE)
        .read_table("observation")
    )
    actual_one_many_output_csv = list(
        parser.Parser(TEST_PARSERS_PATH / "oneToMany.json")
        .parse(TEST_SOURCES_PATH / "oneToMany.csv")
        .read_table("observation")
    )
    assert actual_one_many_output_rows == ONE_MANY_OUTPUT
    assert actual_one_many_output_csv == ONE_MANY_OUTPUT


# HERE
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


def test_exception_get_value_unhashed():
    with pytest.raises(ValueError, match="Could not convert"):
        parser.get_value_unhashed(
            {"age_unit": "years", "age": "a"},
            {"source_unit": {"field": "age_unit"}, "field": "age", "unit": "months"},
        )


def test_invalid_rule():
    with pytest.raises(ValueError, match="Could not return value for"):
        parser.get_value_unhashed({"age_unit": "years", "age": "a"}, {})


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


@pytest.mark.parametrize(
    "rowrule,expected",
    [
        ((ROW_CONCISE, RULE_EXCLUDE), [0, 2]),
        ((ROW_CONCISE, {**RULE_EXCLUDE, "excludeWhen": "false-like"}), [2]),
        ((ROW_CONCISE, {**RULE_EXCLUDE, "excludeWhen": "none"}), [0, 2]),
        ((ROW_CONCISE, {**RULE_EXCLUDE, "excludeWhen": [2]}), [0]),
    ],
)
def test_list_exclude(rowrule, expected):
    assert parser.get_combined_type(*rowrule) == expected


def test_invalid_list_exclude():
    with pytest.raises(
        ValueError,
        match="excludeWhen rule should be 'none', 'false-like', or a list of values",
    ):
        parser.get_combined_type(
            {"modliv": 1, "mildliv": 2},
            {
                "combinedType": "list",
                "fields": [{"field": "modliv"}, {"field": "mildliv"}],
                "excludeWhen": 5,
            },
        )


def test_invalid_combined_type():
    with pytest.raises(ValueError, match="Unknown"):
        parser.get_combined_type(ROW_CONCISE, {"combinedType": "collage", "fields": []})


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
    buf = ps.parse_rows(SOURCE_GROUPBY).write_csv("subject")
    assert buf == snapshot


def test_validation(snapshot):
    ps = parser.Parser(TEST_PARSERS_PATH / "groupBy-with-schema.json")
    buf = ps.parse_rows(SOURCE_GROUPBY_INVALID).write_csv("subject")
    print(buf)
    assert buf == snapshot


def test_multi_id_groupby(snapshot):
    ps = parser.Parser(TEST_PARSERS_PATH / "groupBy-multi-id.json")
    buf = ps.parse_rows(SOURCE_GROUPBY_MULTI_ID).write_csv("subject")
    print(buf)
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
            "groupBy needs aggregation=lastNotNull to be set for table:",
        ),
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
            .parse_rows(ONE_MANY_SOURCE)
            .read_table("obs")
        )


def test_constant_table():
    ps = parser.Parser(TEST_PARSERS_PATH / "constant.json").parse_rows([{"x": 1}])
    assert list(ps.read_table("metadata")) == [
        {"dataset": "constant", "version": "20220505.1", "format": "csv"}
    ]


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
    with (TEST_SCHEMAS_PATH / "epoch-data.schema.json").open() as fp:
        schema = json.load(fp)
    assert schema["required"] == ["epoch", "id", "text"]
    assert parser.make_fields_optional(schema, ["text"])["required"] == ["epoch", "id"]
    assert parser.make_fields_optional(schema, ["field_not_present"])["required"] == [
        "epoch",
        "id",
        "text",
    ]


def test_reference_expansion():
    ps_noref = parser.Parser(TEST_PARSERS_PATH / "groupBy.json")
    ps_ref = parser.Parser(TEST_PARSERS_PATH / "groupBy-defs.json")
    del ps_ref.spec["adtl"]["defs"]
    assert ps_ref.spec == ps_noref.spec


def test_format_equivalence():
    adtl_json = parser.Parser(TEST_PARSERS_PATH / "groupBy-defs.json")
    adtl_toml = parser.Parser(TEST_PARSERS_PATH / "groupBy-defs.toml")
    assert adtl_json.spec == adtl_toml.spec


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


@pytest.mark.parametrize(
    "source,expected",
    [
        (FOR_PATTERN, EXPANDED_FOR_PATTERN),
        (FOR_PATTERN_ANY, EXPANDED_FOR_PATTERN_ANY),
        (FOR_PATTERN_MULTI_VAR, EXPANDED_FOR_PATTERN_MULTI_VAR),
    ],
)
def test_expand_for(source, expected):
    assert parser.expand_for(source) == expected


# write functions to check that apply is working properly
def test_apply_when_values_are_present():
    apply_values_present_output = list(
        parser.Parser(TEST_PARSERS_PATH / "apply.toml")
        .parse_rows(SOURCE_APPLY_PRESENT)
        .read_table("subject")
    )

    assert apply_values_present_output == APPLY_PRESENT_OUTPUT


def test_apply_when_values_not_present():
    apply_values_absent_output = list(
        parser.Parser(TEST_PARSERS_PATH / "apply.toml")
        .parse_rows(SOURCE_APPLY_ABSENT)
        .read_table("subject")
    )

    assert apply_values_absent_output == APPLY_ABSENT_OUTPUT


def test_apply_in_observations_table():
    apply_observations_output = list(
        parser.Parser(TEST_PARSERS_PATH / "apply-observations.toml")
        .parse_rows(APPLY_OBSERVATIONS_SOURCE)
        .read_table("observation")
    )

    assert apply_observations_output == APPLY_OBSERVATIONS_OUTPUT
