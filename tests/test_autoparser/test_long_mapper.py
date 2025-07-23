from enum import Enum
from typing import Optional
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
from pydantic import create_model
from testing_data_animals import TestLLM, long_value_mapping

from adtl.autoparser import LongMapper, setup_config

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
def mapper(config, mock_data_dict):
    mapper = LongMapper(
        data_dictionary=mock_data_dict,
        table_name="vet_observations",
    )

    mapper.model = TestLLM()
    return mapper


@pytest.fixture
def common_fields_mapper(mapper):
    mapper.set_common_fields({"animal_id": "subjid", "visit_date": "Yesterday"})
    return mapper


# --------------------------
# Tests
# --------------------------


def test_check_config_valid(mapper):
    mapper._check_config()  # Should not raise


def test_create_config_failure_no_long_tables(mock_data_dict):
    setup_config(
        {
            "name": "Test Config",
            "max_common_count": 8,
            "language": "en",
            "llm_provider": "openai",
            "api_key": "1234",  # dummy API key
            "schemas": {
                "vet_observations": "tests/test_autoparser/schemas/vet-obs.schema.json"
            },
        }
    )

    with pytest.raises(ValueError, match="No long tables defined in config file"):
        LongMapper(
            data_dictionary=mock_data_dict,
            table_name="vet_observations",
        )._check_config()


def test_create_config_failure_no_enum_fields(mapper):
    del mapper.schema_fields[mapper.variable_col]["enum"]

    with pytest.raises(
        ValueError, match="'observation' in schema does not have an enum set"
    ):
        mapper._check_config()


def test_set_common_fields_valid(common_fields_mapper):
    assert common_fields_mapper.common_fields == {
        "animal_id": "subjid",
        "visit_date": "Yesterday",
    }
    assert common_fields_mapper.common_cols == ["animal_id", "visit_date"]


def test_set_common_fields_mismatch_raises(mapper):
    with pytest.raises(ValueError, match="do not match provided common fields"):
        mapper.set_common_fields({"wrong_field": "DRC"})


def test_common_values_mapped_missing(mapper):
    with pytest.raises(
        AttributeError,
        match="Fields have to be mapped using the `match_fields_to_schema`",
    ):
        mapper.common_values_mapped


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


def test_create_data_model(common_fields_mapper):
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
        "clinic": (
            Optional[Enum("clinicEnum", ["summertown", "jericho", "cowley", "botley"])],
            None,
        ),
    }
    SingleEntry = create_model("SingleEntry", **fields)
    data_model = common_fields_mapper._create_data_model()

    assert (
        SingleEntry.model_json_schema()["properties"]
        == data_model.model_json_schema()["properties"]
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
        assert result.index.name == "source_field"


def test_create_mapping_failure_fields_not_set(mapper):
    with pytest.raises(ValueError, match="Common fields must be set"):
        mapper.create_mapping(save=False)
