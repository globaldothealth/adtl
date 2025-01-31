import pytest
from pytest_unordered import unordered

import adtl.get_value as parser

ROW_CONCISE = {"mildliv": 0, "modliv": 2}
RULE_EXCLUDE = {
    "combinedType": "list",
    "fields": [{"field": "mildliv"}, {"field": "modliv"}],
}


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
