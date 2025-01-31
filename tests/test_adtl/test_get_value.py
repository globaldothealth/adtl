import pytest
from pytest_unordered import unordered

import adtl.get_value as parser

RULE_SINGLE_FIELD = {"field": "diabetes_mhyn"}
RULE_SINGLE_FIELD_WITH_MAPPING = {
    "field": "diabetes_mhyn",
    "values": {"1": True, "2": False, "3": None},
}

ROW_CONCISE = {"mildliv": 0, "modliv": 2}
RULE_EXCLUDE = {
    "combinedType": "list",
    "fields": [{"field": "mildliv"}, {"field": "modliv"}],
}

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

RULE_NON_SENSITIVE = {"field": "id"}
RULE_SENSITIVE = {"field": "id", "sensitive": True}

RULE_IGNOREMISSINGKEY = {
    "field": "diabetes_mhyn",
    "values": {"type 1": "E10", "type 2": "E11"},  # ICD-10 codes
    "ignoreMissingKey": True,
}

RULE_CASEINSENSITIVE = {
    "field": "diabetes_mhyn",
    "values": {"Type 1": "E10", "TYPE 2": "E11"},  # ICD-10 codes
    "caseInsensitive": True,
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

ROW_DATE_BOTH_PRESENT = {"admission_date": "2020-05-05", "enrolment_date": "2020-05-19"}
ROW_DATE_ONLY_ONE = {"admission_date": "2020-05-05", "enrolment_date": ""}

RULE_DATE_MDY = {"field": "outcome_date", "source_date": "%d/%m/%Y", "date": "%m/%d/%Y"}
RULE_DATE_ISO = {"field": "outcome_date", "source_date": "%d/%m/%Y"}

RULE_FIELD_OPTION_SKIP = {
    "field": "aidshiv_mhyn",
    "values": {"1": True, "0": False},
    "can_skip": True,
}


@pytest.mark.parametrize(
    "row_rule,expected",
    [
        (({"diabetes_mhyn": "1"}, RULE_SINGLE_FIELD_WITH_MAPPING), True),
        (({"diabetes_mhyn": "1"}, RULE_SINGLE_FIELD), 1),
        (({}, "CONST"), "CONST"),
        (({"id": "1"}, RULE_NON_SENSITIVE), 1),
        (
            ({"id": "1"}, RULE_SENSITIVE),
            "6b86b273ff34fce19d6b804eff5a3f5747ada4eaa22f1d49c01e52ddb7875b4b",
        ),
        (
            ({"id": 1}, RULE_SENSITIVE),
            "6b86b273ff34fce19d6b804eff5a3f5747ada4eaa22f1d49c01e52ddb7875b4b",
        ),
        (({"diabetes_mhyn": "type 1"}, RULE_IGNOREMISSINGKEY), "E10"),
        (({"diabetes_mhyn": "gestational"}, RULE_IGNOREMISSINGKEY), "gestational"),
        (({"diabetes_mhyn": "type 2"}, RULE_CASEINSENSITIVE), "E11"),
        (({"diabetes_mhyn": "TYPE 1"}, RULE_CASEINSENSITIVE), "E10"),
        ((ROW_CONDITIONAL, RULE_CONDITIONAL_OK), "2022-01-01"),
        ((ROW_CONDITIONAL, RULE_CONDITIONAL_FAIL), None),
        ((ROW_UNIT_MONTH, RULE_UNIT), 1.5),
        ((ROW_UNIT_YEAR, RULE_UNIT), 18),
        (({"outcome_date": "02/05/2022"}, RULE_DATE_MDY), "05/02/2022"),
        (({"outcome_date": "02/05/2022"}, RULE_DATE_ISO), "2022-05-02"),
        (({"outcome_date": "2022-05-02"}, RULE_DATE_ISO), None),
        (({"aidshiv": "1"}, RULE_FIELD_OPTION_SKIP), None),
        (({"aidshiv_mhyn": "1"}, RULE_FIELD_OPTION_SKIP), True),
        (({"aidshiv_mhyn": "2"}, RULE_FIELD_OPTION_SKIP), None),
    ],
)
def test_get_value_single_field(row_rule, expected):
    row, rule = row_rule
    assert parser.get_value(row, rule) == expected


RULE_COMBINED_TYPE_MIN = {  # earliest date
    "combinedType": "min",
    "fields": [{"field": "admission_date"}, {"field": "enrolment_date"}],
}
RULE_COMBINED_TYPE_MAX = {  # latest date
    "combinedType": "max",
    "fields": [{"field": "admission_date"}, {"field": "enrolment_date"}],
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
RULE_COMBINED_TYPE_MIN = {  # earliest date
    "combinedType": "min",
    "fields": [{"field": "admission_date"}, {"field": "enrolment_date"}],
}
RULE_COMBINED_TYPE_MAX = {  # latest date
    "combinedType": "max",
    "fields": [{"field": "admission_date"}, {"field": "enrolment_date"}],
}
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

RULE_COMBINED_TYPE_SET = {
    "combinedType": "set",
    "excludeWhen": "none",
    "fields": ANTIVIRAL_TYPE,
}


@pytest.mark.parametrize(
    "row_rule,expected",
    [
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
        (({"first": "1", "second": ""}, RULE_COMBINED_FIRST_NON_NULL), 1),
        (({"first": "1", "second": "2"}, RULE_COMBINED_FIRST_NON_NULL), 1),
        (({"first": "2", "second": "1"}, RULE_COMBINED_FIRST_NON_NULL), 2),
        (({"first": "", "second": "3"}, RULE_COMBINED_FIRST_NON_NULL), 3),
        (({"first": False, "second": True}, RULE_COMBINED_FIRST_NON_NULL), False),
        (({"first": "", "second": False}, RULE_COMBINED_FIRST_NON_NULL), False),
        ((ROW_DATE_BOTH_PRESENT, RULE_COMBINED_TYPE_MIN), "2020-05-05"),
        ((ROW_DATE_BOTH_PRESENT, RULE_COMBINED_TYPE_MAX), "2020-05-19"),
        ((ROW_DATE_ONLY_ONE, RULE_COMBINED_TYPE_MIN), "2020-05-05"),
        ((ROW_DATE_ONLY_ONE, RULE_COMBINED_TYPE_MAX), "2020-05-05"),
        (({"admission_date": "", "enrolment_date": ""}, RULE_COMBINED_TYPE_MIN), None),
        (({"admission_date": "", "enrolment_date": ""}, RULE_COMBINED_TYPE_MAX), None),
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
def test_get_value_combined_type(row_rule, expected):
    row, rule = row_rule
    assert parser.get_value(row, rule) == expected


@pytest.mark.parametrize(
    "row_rule,expected",
    [
        (({"pathogen": "covid 19"}, {"pathogen": {"=~": ".*covid.*"}}), True),
        (({"pathogen": "covid 19"}, {"pathogen": {"=~": ".*SARS-?CoV-?2.*"}}), False),
        (
            ({"pathogen": "sars cov 2"}, {"pathogen": {"=~": ".*SARS[- ]CoV[- ]2.*"}}),
            True,
        ),
        (
            ({"pathogen": "sars-cov 2"}, {"pathogen": {"=~": ".*SARS[- ]CoV[- ]2.*"}}),
            True,
        ),
        (
            ({"pathogen": "coronavírus"}, {"pathogen": {"=~": ".*coronav[ií]rus.*"}}),
            True,
        ),
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
        ((ROW_CONDITIONAL, {"outcome_date": {"==": 2022}}), False),
        ((ROW_CONDITIONAL, {"outcome_date": 2022}), False),
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


@pytest.mark.parametrize(
    "test_input,expected",
    [
        (
            [None, ["Dexamethasone", "Fluticasone", "Methylprednisolone"]],
            [None, "Dexamethasone", "Fluticasone", "Methylprednisolone"],
        ),
        ([12, ["13", "14"], [[15], ["sixteen"]]], [12, "13", "14", 15, "sixteen"]),
    ],
)
def test_flatten(test_input, expected):
    assert list(parser.flatten(test_input)) == expected


@pytest.mark.parametrize(
    "test_row, test_combination, expected",
    [
        (
            {"corticost": "", "corticost_v2": "Dexa"},
            "set",
            [None, "Dexamethasone"],
        ),
        ({"corticost": "Decadron", "corticost_v2": "Dexa"}, "set", ["Dexamethasone"]),
        (
            {"corticost": "", "corticost_v2": "Cortisonal"},
            "firstNonNull",
            "Cortisonal",
        ),
    ],
)
def test_combinedtype_wordsubstituteset(test_row, test_combination, expected):
    test_rule = {
        "combinedType": test_combination,
        "fields": [
            {
                "field": "corticost",
                "apply": {
                    "function": "wordSubstituteSet",
                    "params": [
                        ["Metil?corten", "Prednisone"],
                        ["Decadron", "Dexamethasone"],
                    ],
                },
            },
            {
                "field": "corticost_v2",
                "apply": {
                    "function": "wordSubstituteSet",
                    "params": [["Cortisonal", "Cortisonal"], ["Dexa", "Dexamethasone"]],
                },
            },
        ],
    }

    assert parser.get_combined_type(test_row, test_rule) == unordered(expected)


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


def test_missing_apply_function():
    with pytest.raises(AttributeError, match="Error using a data transformation"):
        parser.get_value_unhashed(
            {"brthdtc": "2020-02-04", "dsstdat": "2023-04-06"},
            {
                "field": "brthdtc",
                "apply": {"function": "undefinedFunction", "params": ["$dsstdat"]},
            },
        )

    with pytest.raises(AttributeError, match="Error using a data transformation"):
        parser.get_value_unhashed(
            {"brthdtc": "2020-02-04", "dsstdat": "2023-04-06"},
            {"field": "brthdtc", "apply": {"function": "undefinedFunction"}},
        )


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
