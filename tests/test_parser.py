import pytest

import adtl as parser

RULE_SINGLE_FIELD = {"field": "diabetes_mhyn"}
RULE_SINGLE_FIELD_WITH_MAPPING = {
    "field": "diabetes_mhyn",
    "values": {"1": True, "2": False, "3": None},
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

ONE_MANY_SPEC = {
    "name": "sampleOneToMany",
    "description": "One to Many example",
    "tables": {"observation": {"kind": "oneToMany"}},
    "observation": [
        {
            "date": {"field": "dt"},
            "name": "headache",
            "is_present": True,
            "#if": {"headache_cmyn": 1},
        },
        {
            "date": {"field": "dt"},
            "name": "cough",
            "is_present": True,
            "#if": {"cough_cmyn": 1},
        },
        {
            "date": {"field": "dt"},
            "name": "dyspnea",
            "is_present": True,
            "#if": {"dyspnea_cmyn": 1},
        },
    ],
}

ONE_MANY_SOURCE = [
    {"dt": "2022-02-05", "headache_cmyn": 1, "cough_cmyn": 1, "dyspnea_cmyn": 0}
]

ONE_MANY_OUTPUT = [
    {"date": "2022-02-05", "name": "headache", "is_present": True},
    {"date": "2022-02-05", "name": "cough", "is_present": True},
]


@pytest.mark.parametrize(
    "row_rule,expected",
    [
        (({"diabetes_mhyn": "1"}, RULE_SINGLE_FIELD_WITH_MAPPING), True),
        (({"diabetes_mhyn": "1"}, RULE_SINGLE_FIELD), "1"),
        (({}, "CONST"), "CONST"),
        (({"modliv": "1", "mildliver": "0"}, RULE_COMBINED_TYPE_ANY), True),
        (({"modliv": "1", "mildliver": "0"}, RULE_COMBINED_TYPE_ALL), False),
        (({"modliv": "1", "mildliver": "0"}, RULE_COMBINED_TYPE_LIST), [True, False]),
        (
            ({"modliv": "1", "mildliver": "0"}, RULE_COMBINED_TYPE_LIST_PATTERN),
            [True, False],
        ),
        (
            (
                {"modliv": "1", "mildliver": "3"},
                {**RULE_COMBINED_TYPE_LIST_PATTERN, "exclude": "null"},
            ),
            [True],
        ),
        (
            ({"modliv": "1", "mildliver": "3"}, RULE_COMBINED_TYPE_LIST_PATTERN),
            [True, None],
        ),
        (({"id": "1"}, RULE_NON_SENSITIVE), "1"),
        (
            ({"id": "1"}, RULE_SENSITIVE),
            "6b86b273ff34fce19d6b804eff5a3f5747ada4eaa22f1d49c01e52ddb7875b4b",
        ),
        (({"first": "1", "second": ""}, RULE_COMBINED_FIRST_NON_NULL), "1"),
        (({"first": "1", "second": "2"}, RULE_COMBINED_FIRST_NON_NULL), "1"),
        (({"first": "2", "second": "1"}, RULE_COMBINED_FIRST_NON_NULL), "2"),
        (({"first": "", "second": "3"}, RULE_COMBINED_FIRST_NON_NULL), "3"),
        ((ROW_CONDITIONAL, RULE_CONDITIONAL_OK), "2022-01-01"),
        ((ROW_CONDITIONAL, RULE_CONDITIONAL_FAIL), None),
        ((ROW_UNIT_MONTH, RULE_UNIT), 1.5),
        ((ROW_UNIT_YEAR, RULE_UNIT), 18),
    ],
)
def test_get_value(row_rule, expected):
    row, rule = row_rule
    assert parser.get_value(row, rule) == expected


@pytest.mark.parametrize(
    "row_rule,expected",
    [
        ((ROW_CONDITIONAL, {"outcome_type": 4}), True),
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
    actual_one_many_output = list(
        parser.Parser(ONE_MANY_SPEC)
        .parse_rows(ONE_MANY_SOURCE)
        .read_table("observation")
    )
    assert actual_one_many_output == ONE_MANY_OUTPUT


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
        ((ROW_CONCISE, {**RULE_EXCLUDE, "exclude": "falsy"}), [2]),
        ((ROW_CONCISE, {**RULE_EXCLUDE, "exclude": "null"}), [0, 2]),
        ((ROW_CONCISE, {**RULE_EXCLUDE, "exclude": [2]}), [0]),
    ],
)
def test_list_exclude(rowrule, expected):
    assert parser.get_list(*rowrule) == expected


def test_invalid_list_exclude():
    with pytest.raises(
        ValueError, match="exclude rule should be 'null', 'falsy' or a list of values"
    ):
        parser.get_list(
            {"modliv": 1, "mildliv": 2},
            {
                "combinedType": "list",
                "fields": [{"field": "modliv"}, {"field": "mildliv"}],
                "exclude": 5,
            },
        )


def test_invalid_combined_type():
    with pytest.raises(ValueError, match="Unknown"):
        parser.get_combined_type(ROW_CONCISE, {"combinedType": "collage", "fields": []})


def test_validate_spec():
    with pytest.raises(ValueError, match="Specification requires key"):
        ps = parser.Parser(dict())
