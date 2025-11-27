import pandas as pd
import pytest
from shared import parser_path

import adtl
import adtl.parser as parser

missing_mapping = {
    "subjid": ["S001", "S002", "S003"],
    "sex": ["M", "F", "M"],
    "dsstdat": ["2020-06-01", "2020-06-02", "2020-06-03"],
    "hostdat": ["2020-05-20", "2020-05-21", "2020-05-22"],
    "extra_field": [123, 456, 789],
}

field_missing = {
    "subjid": ["S001", "S002", "S003"],
    "dsstdat": ["2020-06-01", "2020-06-02", "2020-06-03"],
    "hostdat": ["2020-05-20", "2020-05-21", "2020-05-22"],
}


def test_get_spec_fields():
    ps = parser.Parser(parser_path / "groupBy.json")
    spec_fields = ps.get_spec_fields()
    assert spec_fields == {"sex", "subjid", "dsstdat", "hostdat"}


@pytest.mark.parametrize(
    "data, expected_missing, expected_absent",
    [
        (
            missing_mapping,
            {"extra_field"},
            set(),
        ),
        (
            field_missing,
            set(),
            {"sex"},
        ),
    ],
    ids=["with extra field", "unmapped field"],
)
def test_check_spec_fields(tmp_path, data, expected_missing, expected_absent):
    ps = parser.Parser(parser_path / "groupBy.json")
    file_path = tmp_path / "test_data.csv"

    pd.DataFrame(data).to_csv(file_path, index=False)

    missing, absent = ps.check_spec_fields(file_path)

    assert missing == expected_missing
    assert absent == expected_absent


def test_check_mapping_error(tmp_path):
    file_path = tmp_path / "test_data.csv"

    pd.DataFrame({}).to_csv(file_path)

    with pytest.raises(
        ValueError, match="There are 1 fields present in your spec file"
    ):
        adtl.check_mapping(parser_path / "oneToMany.json", file_path)


def test_check_mapping_warning(tmp_path):
    file_path = tmp_path / "test_data.csv"

    pd.DataFrame(missing_mapping).to_csv(file_path, index=False)

    with pytest.warns(
        UserWarning, match="There are 1 fields missing from your spec file"
    ):
        adtl.check_mapping(parser_path / "groupBy.json", file_path)
