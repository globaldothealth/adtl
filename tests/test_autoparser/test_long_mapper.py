from enum import Enum
from typing import Optional
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
from pydantic import create_model
from testing_data_animals import TestLLM, long_value_mapping

from adtl.autoparser import LongMapper

# --------------------------
# Fixtures
# --------------------------


@pytest.fixture
def mock_data_dict():
    return pd.DataFrame(
        {
            "source_field": [
                "ID",
                "name",
                "owner",
                "date",
                "clinic_name",
                "weight_kg",
                "temperature_C",
                "vacc_status",
                "reported_issues",
            ],
            "source_description": [
                "Identity number",
                "Name of animal",
                "Name of Owner",
                "Date of visit",
                "Clinic or location",
                "Weight in kg",
                "Temperature in Celsius",
                "Vaccination Status",
                "Reported issues",
            ],
            "source_type": [
                "numeric",
                "string",
                "string",
                "date",
                "string",
                "numeric",
                "numeric",
                "boolean",
                "string",
            ],
            "common_values": [
                None,
                None,
                None,
                None,
                "ST, J, C, B",
                None,
                None,
                "True, False, None",
                None,
            ],
        }
    )


@pytest.fixture
def mock_config():
    return {
        "name": "Test Config",
        "description": "A test configuration for LongMapper",
        "llm_provider": "openai",
        "choice_delimiter": ", ",
        "choice_delimiter_map": "=",
        "num_refs": 3,
        "max_common_count": 8,
        "schemas": {
            "vet_observations": "tests/test_autoparser/schemas/vet-obs.schema.json"
        },
        "column_mappings": {
            "source_field": "Field Name",
            "source_description": "Description",
            "source_type": "Type",
            "common_values": "Common Values",
            "choices": "Choices",
        },
        "long_tables": {
            "vet_observations": {
                "common_cols": ["animal_id", "visit_date", "clinic"],
                "variable_col": "observation",
                "value_cols": ["string_value", "boolean_value", "numeric_value"],
            }
        },
    }


# @pytest.fixture
# def mock_model():
#     model = MagicMock()
#     model.map_long_table.return_value.model_dump.return_value = {
#         "long_table": [
#             {
#                 "source_description": "Number of cases",
#                 "variable_name": "cases",
#                 "value_col": "value_col",
#             }
#         ]
#     }
#     model.map_values.return_value.values = []  # mock for match_values_to_schema
#     return model


@pytest.fixture
def mapper(mock_data_dict, mock_config):
    mapper = LongMapper(
        data_dictionary=mock_data_dict,
        table_name="vet_observations",
        language="en",
        api_key="1234",  # dummy API key
        llm_provider=None,
        llm_model=None,
        config=mock_config,
    )

    mapper.model = TestLLM()
    return mapper


@pytest.fixture
def common_fields_mapper(mapper):
    mapper.set_common_fields(
        {"animal_id": "subjid", "visit_date": "date", "clinic": "jericho"}
    )
    return mapper


# --------------------------
# Tests
# --------------------------


def test_check_config_valid(mapper):
    mapper._check_config()  # Should not raise


def test_set_common_fields_valid(common_fields_mapper):
    assert common_fields_mapper.common_fields == {
        "animal_id": "subjid",
        "visit_date": "date",
        "clinic": "jericho",
    }
    assert common_fields_mapper.common_cols == ["animal_id", "visit_date", "clinic"]


def test_set_common_fields_mismatch_raises(mapper):
    with pytest.raises(ValueError, match="do not match provided common fields"):
        mapper.set_common_fields({"wrong_field": "DRC"})


def test_target_values(mapper):
    pd.testing.assert_series_equal(
        mapper.target_values,
        pd.Series(
            {
                "string_value": np.nan,
                "boolean_value": ["True", "False", "None"],
                "numeric_value": np.nan,
            }
        ),
    )


def test_create_data_structure(common_fields_mapper):
    fields = {
        "source_description": (str, ...),
        "variable_name": (
            Optional[
                Enum(
                    "VarColEnum",
                    [
                        "weight",
                        "temperature",
                        "vaccinated",
                        "neutered",
                        "pregnant",
                        "arthritis",
                        "behavioural_issue",
                    ],
                )
            ],
            None,
        ),
        "value_col": (
            Optional[
                Enum("ValueColEnum", ["string_value", "boolean_value", "numeric_value"])
            ],
            None,
        ),
        "vet_name": (Optional[str], None),
    }
    SingleEntry = create_model("SingleEntry", **fields)
    data_structure = common_fields_mapper._create_data_structure()

    assert (
        SingleEntry.model_json_schema()["properties"]
        == data_structure.model_json_schema()["properties"]
    )


def test_match_fields_to_schema(common_fields_mapper):
    df = common_fields_mapper.match_fields_to_schema()
    assert isinstance(df, pd.DataFrame)
    assert "variable_name" in df.columns
    assert df.loc["weight_kg", "variable_name"] == "weight"
    assert len(df) == 9  # Should match the number of source fields


def test_iter_value_tuples(common_fields_mapper):
    common_fields_mapper.match_fields_to_schema()
    tuples = list(common_fields_mapper._iter_value_tuples())
    assert len(tuples) == 1
    assert tuples[0][0] == "vacc_status"
    assert tuples[0][-1] == ["True", "False", "None"]


@pytest.mark.filterwarnings("ignore:The following fields have not been mapped")
def test_create_mapping_success(common_fields_mapper):
    with patch(
        "testing_data_animals.TestLLM.map_values",
        return_value=long_value_mapping,
    ):
        result = common_fields_mapper.create_mapping(save=False)
        assert isinstance(result, pd.DataFrame)
        # assert "country" in result.columns
        # assert "variable_name" in result.columns
        assert result.index.name == "source_field"


# def test_create_mapping_warns_on_unmapped(mapper):
#     mapper.model.map_long_table.return_value.model_dump.return_value = {
#         "long_table": []
#     }
#     mapper.set_common_fields({"country": "DRC", "year": "2023"})
#     with pytest.warns(UserWarning, match="have not been mapped to the new schema"):
#         mapper.create_mapping(save=False)


# def test_missing_long_tables_raises(mock_data_dict, mock_schema):
#     config = {"language": "en", "schemas": {"test_table": "fake_schema.json"}}
#     with (
#         patch("adtl.autoparser.util.read_config_schema", return_value=config),
#         patch("adtl.autoparser.util.read_json", return_value=mock_schema),
#         patch("adtl.autoparser.dict_reader.format_dict", return_value=mock_data_dict),
#     ):

#         with pytest.raises(ValueError, match="No long tables defined in config file"):
#             LongMapper(
#                 data_dictionary=mock_data_dict,
#                 table_name="test_table",
#                 api_key="fake",
#                 language="en",
#             )._check_config()


# def test_missing_table_name_raises(mock_data_dict):
#     with pytest.raises(ValueError, match="not defined in config file"):
#         LongMapper(
#             data_dictionary=mock_data_dict,
#             table_name="missing_table",
#             language="en",
#             api_key="1234",  # dummy API key
#             llm_provider=None,
#             llm_model=None,
#             config="tests/test_autoparser/test_config.toml",
#         )._check_config()


def test_missing_variable_col_raises(mock_data_dict, mock_config):
    config = mock_config
    del config["long_tables"]["vet_observations"]["variable_col"]

    with pytest.raises(ValueError, match="Variable column not set in config"):
        LongMapper(
            data_dictionary=mock_data_dict,
            table_name="vet_observations",
            language="en",
            api_key="1234",  # dummy API key
            llm_provider=None,
            llm_model=None,
            config=config,
        )._check_config()
