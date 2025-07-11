from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from adtl.autoparser import setup_config
from adtl.autoparser.make_toml import LongTableParser


@pytest.fixture(autouse=True)
def config():
    """Fixture to load the configuration for the autoparser."""
    setup_config(
        {
            "name": "test_autoparser",
            "language": "en",
            "max_common_count": 8,
            "schemas": {
                "vet_observations": "tests/test_autoparser/schemas/vet-obs.schema.json"
            },
            "long_tables": {
                "vet_observations": {
                    "common_fields": {
                        "animal_id": "subjid",
                        "visit_date": "date",
                        "clinic": "jericho",
                    },
                    "variable_col": "observation",
                    "value_cols": ["string_value", "boolean_value", "numeric_value"],
                }
            },
        }
    )


@pytest.fixture
def long_parser():
    return LongTableParser(
        mapping=pd.read_csv("tests/test_autoparser/sources/long-animal-mapper.csv"),
        schema=json.load(
            Path("tests/test_autoparser/schemas/vet-obs.schema.json").open()
        ),
        table_name="vet_observations",
    )


def test_constant_fields(long_parser):
    expected = {
        "animal_id": False,
        "visit_date": False,
        "observation": True,
        "string_value": False,
        "boolean_value": False,
        "numeric_value": False,
        "vet_name": True,
        "clinic": False,
    }

    assert long_parser.constant_field == expected


def test_update_constant_fields(long_parser):
    long_parser.update_constant_fields({"clinic": True})

    assert long_parser.constant_field["clinic"] is True


@pytest.mark.parametrize(
    "mapping_dict,match",
    [
        (
            pd.DataFrame(
                {
                    "source_field": ["animal_id", "visit_date"],
                    "observation": ["date", np.nan],
                    "value_col": ["string_value", "string_value"],
                }
            ),
            "must not contain NaN values in 'observation' column.",
        ),
        (
            pd.DataFrame(
                {
                    "source_field": ["animal_id", "visit_date"],
                    "observation": ["subjid", "date"],
                    "value_col": [np.nan, "string_value"],
                }
            ),
            "NaN values in the 'value_col' column.",
        ),
    ],
)
def test_map_validation(mapping_dict, match):
    with pytest.raises(ValueError, match=match):
        LongTableParser(
            mapping=pd.DataFrame(mapping_dict),
            schema=json.load(
                Path("tests/test_autoparser/schemas/vet-obs.schema.json").open()
            ),
            table_name="vet_observations",
        )._validate_mapping()


@pytest.mark.parametrize(
    "row, expected",
    [
        (
            pd.Series(
                data=[
                    "weight_kg",
                    "Weight in kg",
                    np.nan,
                    "weight",
                    "numeric_value",
                    "Dr. Lopez",
                    np.nan,
                    "subjid",
                    "date",
                    "clinic_name",
                ],
                index=[
                    "source_field",
                    "source_description",
                    "common_values",
                    "observation",
                    "value_col",
                    "vet_name",
                    "value_mapping",
                    "animal_id",
                    "visit_date",
                    "clinic",
                ],
            ),
            {
                "animal_id": {"field": "subjid"},
                "visit_date": {"field": "date"},
                "observation": "weight",
                "numeric_value": {"field": "weight_kg"},
                "vet_name": "Dr. Lopez",
                "clinic": {"field": "clinic_name"},
            },
        ),
        (
            pd.Series(
                data=[
                    "vacc_status",
                    "Vaccination Status",
                    "false, none, true",
                    "vaccinated",
                    "boolean_value",
                    "Dr. Lopez",
                    "true=True, false=False",
                    "subjid",
                    "date",
                    "clinic_name",
                ],
                index=[
                    "source_field",
                    "source_description",
                    "common_values",
                    "observation",
                    "value_col",
                    "vet_name",
                    "value_mapping",
                    "animal_id",
                    "visit_date",
                    "clinic",
                ],
            ),
            {
                "animal_id": {"field": "subjid"},
                "visit_date": {"field": "date"},
                "observation": "vaccinated",
                "boolean_value": {
                    "field": "vacc_status",
                    "values": {"true": True, "false": False},
                    "caseInsensitive": True,
                },
                "vet_name": "Dr. Lopez",
                "clinic": {"field": "clinic_name"},
            },
        ),
    ],
)
def test_single_entry_mapping(row, expected, long_parser):
    assert long_parser.single_entry_mapping(row) == expected


def test_make_table(long_parser, snapshot):
    long_parser.update_constant_fields({"clinic": True})

    long_table, _ = long_parser.make_toml_table()

    snapshot.assert_match(long_table)
